"""
StatefulFeatureEngine — v32 Walk-Forward Feature Builder (v2)
==============================================================
Phase 55.3 / 55.4 implementation.

CHANGES FROM v1 (smoke-tested version):
  1. PER-DAY PAGERANK (fidelity fix). v31's EquineNetworkEngineer computed
     PageRank per-day (groupby 'date'), snapshotting the network BEFORE the
     day's results were added. The v1 engine recomputed per-race. This v2
     adds an opt-in per-day freeze mode (use_daily_pr_freeze) that matches
     v31 exactly AND is ~10x faster. Per-race lazy mode remains the default
     so the v1 smoke test behavior is preserved when the flag is off.
  2. horse_no added to _load_race query and to the snapshot, so the
     execution desk + settlement can map horse_id -> horse_no without
     per-race DB queries.

Faithfully replicates the v31 (V12 Matrix) feature engineering:
  MarginAdjustedElo / Glicko-2 / per-day PageRank / SectionalPace /
  HumanMomentum / physical deltas.

LEAKAGE PROTOCOL (unchanged):
  snapshot_for(race_id) BEFORE advance_race(race_id), race-by-race in
  chronological order. Snapshots read only prior-race state.
"""

import math
import logging
import sqlite3
from collections import defaultdict

import numpy as np
import pandas as pd
import networkx as nx

log = logging.getLogger(__name__)

HUMAN_BASELINE = 0.083


class StatefulFeatureEngine:

    ELO_K_BASE     = 20.0
    ELO_INIT       = 1500.0

    GLICKO_TAU     = 0.5
    GLICKO_SCALE   = 173.7178
    GLICKO_INIT_R  = 1500.0
    GLICKO_INIT_RD = 350.0
    GLICKO_INIT_V  = 0.06

    PAGERANK_DAMP  = 0.85
    PAGERANK_DEF   = 1.0 / 1000.0

    PACE_WINDOW    = 5
    HUMAN_WINDOW   = 30

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._race_cache = {}
        self.use_daily_pr_freeze = False   # opt-in per-day PageRank
        self.reset()

    # =================================================================
    # STATE
    # =================================================================
    def reset(self):
        self.elo = {}
        self.g_r, self.g_rd, self.g_vol = {}, {}, {}
        self.pr_graph = nx.DiGraph()
        self.pr_cache = {}
        self.pr_dirty = True
        self.frozen_pr = {}                # used in per-day freeze mode
        self.pace_hist = defaultdict(list)
        self.jockey_hist = defaultdict(list)
        self.trainer_hist = defaultdict(list)
        self.horse_phys = {}
        log.debug("StatefulFeatureEngine reset.")

    # =================================================================
    # DATA
    # =================================================================
    def _load_race(self, race_id: str) -> pd.DataFrame:
        if race_id in self._race_cache:
            return self._race_cache[race_id]
        q = """
            SELECT race_id, date_iso, race_no, horse_id, horse_no, horse_name,
                   finish_position, jockey, trainer, act_wt, draw,
                   distance, course, lbw, running_pos, win_odds
            FROM race_results
            WHERE race_id = ?
            ORDER BY finish_position
        """
        df = pd.read_sql(q, self.conn, params=(race_id,))
        self._race_cache[race_id] = df
        return df

    # =================================================================
    # PARSERS (exact v31)
    # =================================================================
    @staticmethod
    def _parse_lbw(lbw_str) -> float:
        if pd.isna(lbw_str) or str(lbw_str).strip() in ['---', '-', '']:
            return 0.0
        s = str(lbw_str).strip().upper()
        if s in ['N', 'NOSE']:  return 0.05
        if s in ['SH', 'SHD', 'SN']: return 0.1
        if s in ['HD']: return 0.2
        if s in ['DH']: return 0.0
        try:
            if '-' in s:
                parts = s.split('-')
                return float(parts[0]) + (float(parts[1].split('/')[0]) /
                                          float(parts[1].split('/')[1]))
            elif '/' in s:
                return float(s.split('/')[0]) / float(s.split('/')[1])
            else:
                return float(s)
        except Exception:
            return 0.0

    @staticmethod
    def _parse_running_pos(pos_string) -> list:
        if pd.isna(pos_string) or pos_string == '---':
            return []
        return [int(x) for x in str(pos_string).split() if x.isdigit()]

    def _g2_transform(self, r, rd):
        return (r - self.GLICKO_INIT_R) / self.GLICKO_SCALE, rd / self.GLICKO_SCALE

    @staticmethod
    def _g_phi(phi):
        return 1.0 / math.sqrt(1.0 + 3.0 * phi**2 / (math.pi**2))

    def _E(self, mu, mu_j, phi_j):
        return 1.0 / (1.0 + math.exp(-self._g_phi(phi_j) * (mu - mu_j)))

    # =================================================================
    # PAGERANK
    # =================================================================
    def freeze_daily_pagerank(self):
        """Recompute PageRank from the CURRENT graph (which contains edges
        through the previous day) and store as the frozen snapshot used for
        all of today's races. Call at each day boundary BEFORE snapshotting
        the day's races. This matches v31's per-day groupby behavior."""
        if self.pr_graph.number_of_nodes() == 0:
            self.frozen_pr = {}
            return
        try:
            self.frozen_pr = nx.pagerank(self.pr_graph, alpha=self.PAGERANK_DAMP)
        except nx.PowerIterationFailedConvergence:
            n = self.pr_graph.number_of_nodes()
            self.frozen_pr = {node: 1.0 / n for node in self.pr_graph.nodes()}

    def _pagerank_snapshot(self) -> dict:
        if self.use_daily_pr_freeze:
            return self.frozen_pr
        # legacy per-race lazy mode (used by v1 smoke test)
        if not self.pr_dirty:
            return self.pr_cache
        if self.pr_graph.number_of_nodes() == 0:
            self.pr_cache = {}
        else:
            try:
                self.pr_cache = nx.pagerank(self.pr_graph, alpha=self.PAGERANK_DAMP)
            except nx.PowerIterationFailedConvergence:
                n = self.pr_graph.number_of_nodes()
                self.pr_cache = {node: 1.0 / n for node in self.pr_graph.nodes()}
        self.pr_dirty = False
        return self.pr_cache

    # =================================================================
    # SNAPSHOT
    # =================================================================
    def snapshot_for(self, race_id: str) -> pd.DataFrame:
        race = self._load_race(race_id)
        if race.empty:
            return pd.DataFrame()

        pr_now = self._pagerank_snapshot()

        esi_vals = {}
        for _, r in race.iterrows():
            esi_vals[r['horse_id']] = self._rolling_pace(r['horse_id'])[0]
        esi_series = pd.Series(esi_vals, dtype=float)
        race_esi_pressure = esi_series.nlargest(3).sum() if len(esi_series) else 0.0

        rows = []
        for _, r in race.iterrows():
            hid = r['horse_id']
            roll_esi, roll_csi = self._rolling_pace(hid)
            phys = self._physical_snapshot(hid, r)
            rows.append({
                'race_id':              race_id,
                'date_iso':             r['date_iso'],
                'race_no':              r['race_no'],
                'horse_id':             hid,
                'horse_no':             r['horse_no'],
                'horse_name':           r['horse_name'],
                'finish_position':      r['finish_position'],
                'jockey':               r['jockey'],
                'trainer':              r['trainer'],
                'win_odds':             r['win_odds'],
                'distance':             r['distance'],
                'pre_race_elo':         self.elo.get(hid, self.ELO_INIT),
                'pre_race_glicko_mu':   self.g_r.get(hid, self.GLICKO_INIT_R),
                'pre_race_glicko_rd':   self.g_rd.get(hid, self.GLICKO_INIT_RD),
                'pre_race_glicko_vol':  self.g_vol.get(hid, self.GLICKO_INIT_V),
                'pre_race_pagerank':    pr_now.get(hid, self.PAGERANK_DEF),
                'shifted_rolling_ESI':  roll_esi,
                'shifted_rolling_CSI':  roll_csi,
                'race_ESI_pressure':    race_esi_pressure,
                'pace_advantage':       (roll_esi - race_esi_pressure)
                                        if not pd.isna(roll_esi) else np.nan,
                'jockey_win_pct':       self._human_pct(self.jockey_hist, r['jockey']),
                'trainer_win_pct':      self._human_pct(self.trainer_hist, r['trainer']),
                'draw':                 self._safe_draw(r['draw']),
                'days_since_last_run':  phys['days_rest'],
                'weight_delta':         phys['weight_delta'],
                'distance_delta':       phys['distance_delta'],
                'career_wins':          phys['career_wins'],
                'is_turf':              1 if 'TURF' in str(r['course']).upper() else 0,
            })
        return pd.DataFrame(rows)

    def _rolling_pace(self, horse_id):
        hist = self.pace_hist.get(horse_id, [])
        if not hist:
            return (np.nan, np.nan)
        window = hist[-self.PACE_WINDOW:]
        esis = [e for (e, c) in window if not pd.isna(e)]
        csis = [c for (e, c) in window if not pd.isna(c)]
        roll_esi = float(np.mean(esis)) if esis else np.nan
        roll_csi = float(np.mean(csis)) if csis else np.nan
        return (roll_esi, roll_csi)

    def _human_pct(self, hist_dict, name):
        hist = hist_dict.get(name, [])
        if not hist:
            return HUMAN_BASELINE
        window = hist[-self.HUMAN_WINDOW:]
        return float(np.mean(window)) if window else HUMAN_BASELINE

    def _physical_snapshot(self, horse_id, row):
        phys = self.horse_phys.get(horse_id)
        if phys is None:
            return {'days_rest': 30.0, 'weight_delta': 0.0,
                    'distance_delta': 0.0, 'career_wins': 0.0}
        cur_date = pd.to_datetime(row['date_iso'])
        days_rest = (cur_date - phys['last_date']).days if phys['last_date'] is not None else 30.0
        cur_wt = self._safe_float(row['act_wt'])
        weight_delta = (cur_wt - phys['last_weight']) if (phys['last_weight'] is not None and cur_wt is not None) else 0.0
        cur_dist = self._safe_float(row['distance'])
        distance_delta = (cur_dist - phys['last_distance']) if (phys['last_distance'] is not None and cur_dist is not None) else 0.0
        return {
            'days_rest': float(days_rest),
            'weight_delta': float(weight_delta),
            'distance_delta': float(distance_delta),
            'career_wins': float(phys['wins']),
        }

    @staticmethod
    def _safe_draw(d):
        try:
            if pd.isna(d): return 7.0
            return float(d)
        except Exception:
            return 7.0

    @staticmethod
    def _safe_float(x):
        try:
            if pd.isna(x): return None
            return float(x)
        except Exception:
            return None

    # =================================================================
    # ADVANCE
    # =================================================================
    def advance_race(self, race_id: str):
        race = self._load_race(race_id)
        if race.empty:
            return

        horses    = race['horse_id'].tolist()
        positions = pd.to_numeric(race['finish_position'], errors='coerce').fillna(99.0).tolist()
        margins   = race['lbw'].apply(self._parse_lbw).tolist()
        n = len(horses)

        # ELO
        for h in horses:
            self.elo.setdefault(h, self.ELO_INIT)
        if n > 1:
            updates = {h: 0.0 for h in horses}
            for i in range(n):
                for j in range(n):
                    if i == j:
                        continue
                    ha, hb = horses[i], horses[j]
                    pa, pb = positions[i], positions[j]
                    ma, mb = margins[i], margins[j]
                    ea_elo, eb_elo = self.elo[ha], self.elo[hb]
                    s_a = 1.0 if pa < pb else (0.0 if pa > pb else 0.5)
                    e_a = 1.0 / (1.0 + math.pow(10, (eb_elo - ea_elo) / 400.0))
                    movm = 1.0 + math.log(abs(ma - mb) + 1.0)
                    updates[ha] += ((self.ELO_K_BASE * movm) * (s_a - e_a)) / (n - 1)
            for h in horses:
                self.elo[h] += updates[h]

        # GLICKO-2 (faithful to v31, incl. its simplified vol)
        for h in horses:
            if h not in self.g_r:
                self.g_r[h], self.g_rd[h], self.g_vol[h] = (
                    self.GLICKO_INIT_R, self.GLICKO_INIT_RD, self.GLICKO_INIT_V)
        if n >= 2:
            g_updates = {}
            for i in range(n):
                ha, pa = horses[i], positions[i]
                mu_a, phi_a = self._g2_transform(self.g_r[ha], self.g_rd[ha])
                vol_a = self.g_vol[ha]
                v_inv, delta_sum = 0.0, 0.0
                for j in range(n):
                    if i == j:
                        continue
                    hb, pb = horses[j], positions[j]
                    mu_b, phi_b = self._g2_transform(self.g_r[hb], self.g_rd[hb])
                    s = 1.0 if pa < pb else (0.0 if pa > pb else 0.5)
                    g_j = self._g_phi(phi_b)
                    e_j = self._E(mu_a, mu_b, phi_b)
                    v_inv += (g_j**2) * e_j * (1.0 - e_j)
                    delta_sum += g_j * (s - e_j)
                v = 1.0 / v_inv if v_inv > 0 else 1.0
                phi_star = math.sqrt(phi_a**2 + vol_a**2)
                phi_prime = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)
                mu_prime = mu_a + (phi_prime**2) * delta_sum
                g_updates[ha] = {
                    'r': (mu_prime * self.GLICKO_SCALE + self.GLICKO_INIT_R),
                    'rd': phi_prime * self.GLICKO_SCALE,
                }
            for h, d in g_updates.items():
                self.g_r[h], self.g_rd[h] = d['r'], d['rd']

        # PAGERANK GRAPH (edges added per-race; PR recomputed per-day in freeze mode)
        if n > 1:
            for i in range(n):
                for j in range(n):
                    if i == j:
                        continue
                    pa, pb = positions[i], positions[j]
                    if pa < pb:
                        loser, winner = horses[j], horses[i]
                        if self.pr_graph.has_edge(loser, winner):
                            self.pr_graph[loser][winner]['weight'] += 1.0
                        else:
                            self.pr_graph.add_edge(loser, winner, weight=1.0)
            self.pr_dirty = True

        # PACE HISTORY
        for _, r in race.iterrows():
            pos = self._parse_running_pos(r['running_pos'])
            raw_esi = (1.0 / math.sqrt(pos[0])) if (len(pos) > 0 and pos[0] > 0) else np.nan
            raw_csi = (pos[-2] - pos[-1]) if len(pos) >= 2 else 0
            self.pace_hist[r['horse_id']].append((raw_esi, raw_csi))

        # HUMAN MOMENTUM
        for _, r in race.iterrows():
            is_win = 1 if pd.to_numeric(r['finish_position'], errors='coerce') == 1 else 0
            if r['jockey'] is not None and str(r['jockey']).strip():
                self.jockey_hist[r['jockey']].append(is_win)
            if r['trainer'] is not None and str(r['trainer']).strip():
                self.trainer_hist[r['trainer']].append(is_win)

        # PHYSICAL
        for _, r in race.iterrows():
            hid = r['horse_id']
            cur_date = pd.to_datetime(r['date_iso'])
            cur_wt = self._safe_float(r['act_wt'])
            cur_dist = self._safe_float(r['distance'])
            is_win = 1 if pd.to_numeric(r['finish_position'], errors='coerce') == 1 else 0
            prev = self.horse_phys.get(hid, {'wins': 0})
            self.horse_phys[hid] = {
                'last_date': cur_date,
                'last_weight': cur_wt,
                'last_distance': cur_dist,
                'wins': prev.get('wins', 0) + is_win,
            }
