"""
StatefulFeatureEngine — v32 Walk-Forward Feature Builder
=========================================================
Phase 55.3 implementation.

Faithfully replicates the v31 (V12 Matrix) feature engineering:
  - MarginAdjustedElo  (k_base=20, MOV multiplier 1+ln(|m_a-m_b|+1))
  - Glicko-2           (tau=0.5, scale 173.7178, init 1500/350/0.06)
  - EquineNetwork PageRank (damping=0.85, per-day directed graph)
  - SectionalPace      (ESI=1/sqrt(pos1), CSI=pos[-2]-pos[-1], roll 5)
  - HumanMomentum      (jockey/trainer rolling-30 win%, fillna 0.083)
  - Physical deltas    (days_rest, weight_delta, distance_delta,
                        career_wins, is_turf)

LEAKAGE PROTOCOL — THE CORE CONTRACT
------------------------------------
The engine maintains running state. For each race, you MUST:
    1. snapshot_for(race_id)  -> returns pre-race feature values
    2. advance_race(race_id)  -> updates state using that race's results

snapshot_for() reads only state accumulated from PRIOR races.
advance_race() folds the current race into the state.
Calling them in this order, race-by-race in chronological order,
guarantees no future race ever influences a past feature value.

This is the walk-forward equivalent of v31's .shift(1) discipline,
but enforced structurally rather than via pandas shift.

DETERMINISM
-----------
All updates are deterministic given identical input order. Races are
processed in (date_iso, race_no) order. No randomness in the engine
itself (XGBoost random_state is set separately in the model trainer).

USAGE
-----
    eng = StatefulFeatureEngine(conn)
    eng.reset()
    for race_id in chronological_race_ids:
        snap = eng.snapshot_for(race_id)   # pre-race features (DataFrame)
        # ... use snap for training or inference ...
        eng.advance_race(race_id)          # fold result into state
"""

import math
import logging
import sqlite3
from collections import defaultdict

import numpy as np
import pandas as pd
import networkx as nx

log = logging.getLogger(__name__)

# Baseline win% for jockeys/trainers with no history (v31 HumanMomentumEngineer)
HUMAN_BASELINE = 0.083


class StatefulFeatureEngine:
    """Maintains running Elo/Glicko/PageRank/momentum/physical state and
    emits pre-race snapshots under the snapshot-then-advance protocol."""

    # ---- v31 constants (exact) ----
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
        self._race_cache = {}   # race_id -> race rows DataFrame
        self._meta_cache = {}   # race_id -> dict(distance, course, date_iso, race_no)
        self.reset()

    # =================================================================
    # STATE MANAGEMENT
    # =================================================================
    def reset(self):
        """Clear all running state. Call before each walk-forward rebuild."""
        # Elo
        self.elo = {}
        # Glicko-2
        self.g_r, self.g_rd, self.g_vol = {}, {}, {}
        # PageRank — accumulates a directed "who-beat-whom" graph
        self.pr_graph = nx.DiGraph()
        self.pr_cache = {}        # horse_id -> last computed pagerank
        self.pr_dirty = True      # recompute PR lazily when graph changes
        # Pace history: horse_id -> list of (raw_ESI, raw_CSI) in chrono order
        self.pace_hist = defaultdict(list)
        # Human momentum: jockey/trainer -> list of is_win (chrono order)
        self.jockey_hist = defaultdict(list)
        self.trainer_hist = defaultdict(list)
        # Physical: horse_id -> dict(last_date, last_weight, last_distance, wins)
        self.horse_phys = {}
        log.debug("StatefulFeatureEngine reset.")

    # =================================================================
    # DATA ACCESS (cached)
    # =================================================================
    def _load_race(self, race_id: str) -> pd.DataFrame:
        if race_id in self._race_cache:
            return self._race_cache[race_id]
        q = """
            SELECT race_id, date_iso, race_no, horse_id, horse_name,
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
    # PARSERS (exact v31 logic)
    # =================================================================
    @staticmethod
    def _parse_lbw(lbw_str) -> float:
        """v31 MarginAdjustedEloEngine._parse_lbw, verbatim."""
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
        """v31 SectionalPaceEngineer._parse_running_pos, verbatim."""
        if pd.isna(pos_string) or pos_string == '---':
            return []
        return [int(x) for x in str(pos_string).split() if x.isdigit()]

    # ---- Glicko-2 helpers (exact v31) ----
    def _g2_transform(self, r, rd):
        return (r - self.GLICKO_INIT_R) / self.GLICKO_SCALE, rd / self.GLICKO_SCALE

    @staticmethod
    def _g_phi(phi):
        return 1.0 / math.sqrt(1.0 + 3.0 * phi**2 / (math.pi**2))

    def _E(self, mu, mu_j, phi_j):
        return 1.0 / (1.0 + math.exp(-self._g_phi(phi_j) * (mu - mu_j)))

    # =================================================================
    # SNAPSHOT — pre-race feature values (reads prior state only)
    # =================================================================
    def snapshot_for(self, race_id: str) -> pd.DataFrame:
        """Return a DataFrame of pre-race feature values for every horse
        in this race. Uses ONLY state accumulated from prior races."""
        race = self._load_race(race_id)
        if race.empty:
            return pd.DataFrame()

        rows = []
        # Precompute PageRank once if the graph changed since last snapshot
        pr_now = self._pagerank_snapshot()

        # Race-level pace pressure needs each horse's shifted_rolling_ESI first
        esi_vals = {}
        for _, r in race.iterrows():
            hid = r['horse_id']
            esi_vals[hid] = self._rolling_pace(hid)[0]  # shifted_rolling_ESI

        # top-3 ESI sum (race_ESI_pressure) — v31 uses nlargest(3).sum()
        esi_series = pd.Series(esi_vals, dtype=float)
        race_esi_pressure = esi_series.nlargest(3).sum() if len(esi_series) else 0.0

        for _, r in race.iterrows():
            hid = r['horse_id']
            roll_esi, roll_csi = self._rolling_pace(hid)
            phys = self._physical_snapshot(hid, r)

            rows.append({
                'race_id':              race_id,
                'date_iso':             r['date_iso'],
                'race_no':              r['race_no'],
                'horse_id':             hid,
                'horse_name':           r['horse_name'],
                'finish_position':      r['finish_position'],
                'jockey':               r['jockey'],
                'trainer':              r['trainer'],
                'win_odds':             r['win_odds'],
                'distance':             r['distance'],
                # --- model features ---
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

    # ---- snapshot helpers ----
    def _rolling_pace(self, horse_id):
        """shifted_rolling mean of last up-to-5 PRIOR (ESI, CSI) values.
        The 'shift' is implicit: pace_hist contains only prior races
        because advance_race appends AFTER snapshot."""
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
        """Rolling-30 win% of PRIOR rides/runs. fillna 0.083."""
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
    # PAGERANK
    # =================================================================
    def _pagerank_snapshot(self) -> dict:
        """Recompute PageRank only if the graph changed since last call."""
        if not self.pr_dirty:
            return self.pr_cache
        if self.pr_graph.number_of_nodes() == 0:
            self.pr_cache = {}
        else:
            try:
                self.pr_cache = nx.pagerank(self.pr_graph, alpha=self.PAGERANK_DAMP)
            except nx.PowerIterationFailedConvergence:
                # Fallback: uniform
                n = self.pr_graph.number_of_nodes()
                self.pr_cache = {node: 1.0 / n for node in self.pr_graph.nodes()}
        self.pr_dirty = False
        return self.pr_cache

    # =================================================================
    # ADVANCE — fold this race's results into running state
    # =================================================================
    def advance_race(self, race_id: str):
        """Update all stateful features using this race's outcome.
        MUST be called AFTER snapshot_for(race_id)."""
        race = self._load_race(race_id)
        if race.empty:
            return

        horses    = race['horse_id'].tolist()
        positions = pd.to_numeric(race['finish_position'], errors='coerce').fillna(99.0).tolist()
        margins   = race['lbw'].apply(self._parse_lbw).tolist()
        n = len(horses)

        # ---------- ELO (Margin-Adjusted, exact v31) ----------
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

        # ---------- GLICKO-2 (exact v31, including its simplified vol) ----------
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

        # ---------- PAGERANK GRAPH (who-beat-whom edges) ----------
        # v31 EquineNetworkEngineer: directed edge loser -> winner per pair,
        # accumulated across the whole history. Graph snapshotted per-day,
        # but since we recompute lazily, we just mark dirty after edges added.
        if n > 1:
            for i in range(n):
                for j in range(n):
                    if i == j:
                        continue
                    pa, pb = positions[i], positions[j]
                    if pa < pb:
                        # i beat j  -> edge from loser j to winner i
                        loser, winner = horses[j], horses[i]
                        if self.pr_graph.has_edge(loser, winner):
                            self.pr_graph[loser][winner]['weight'] += 1.0
                        else:
                            self.pr_graph.add_edge(loser, winner, weight=1.0)
            self.pr_dirty = True

        # ---------- PACE HISTORY ----------
        for _, r in race.iterrows():
            pos = self._parse_running_pos(r['running_pos'])
            raw_esi = (1.0 / math.sqrt(pos[0])) if (len(pos) > 0 and pos[0] > 0) else np.nan
            raw_csi = (pos[-2] - pos[-1]) if len(pos) >= 2 else 0
            self.pace_hist[r['horse_id']].append((raw_esi, raw_csi))

        # ---------- HUMAN MOMENTUM ----------
        for _, r in race.iterrows():
            is_win = 1 if pd.to_numeric(r['finish_position'], errors='coerce') == 1 else 0
            if r['jockey'] is not None and str(r['jockey']).strip():
                self.jockey_hist[r['jockey']].append(is_win)
            if r['trainer'] is not None and str(r['trainer']).strip():
                self.trainer_hist[r['trainer']].append(is_win)

        # ---------- PHYSICAL ----------
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


# =====================================================================
# Self-test / smoke test
# =====================================================================
if __name__ == "__main__":
    import os
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB = os.path.join(_ROOT, "data", "hk_racing.db")
    conn = sqlite3.connect(DB)

    # Pull the first ~200 chronological bettable race_ids as a smoke test
    q = """
        SELECT DISTINCT m.race_id, m.date_iso, m.race_no
        FROM race_metadata m
        WHERE m.is_bettable = 1
        ORDER BY m.date_iso, m.race_no
        LIMIT 200
    """
    races = pd.read_sql(q, conn)
    log.info(f"Smoke test on {len(races)} earliest races")

    eng = StatefulFeatureEngine(conn)
    eng.reset()

    all_snaps = []
    for _, row in races.iterrows():
        snap = eng.snapshot_for(row['race_id'])
        all_snaps.append(snap)
        eng.advance_race(row['race_id'])

    result = pd.concat(all_snaps, ignore_index=True)
    log.info(f"Generated {len(result)} horse-race feature rows")
    log.info("\nFeature columns:\n" + str(list(result.columns)))
    log.info("\nElo distribution after 200 races:")
    log.info(result['pre_race_elo'].describe().to_string())
    log.info("\nSample of last race's snapshot:")
    log.info(result.tail(10)[['horse_id', 'pre_race_elo', 'pre_race_glicko_mu',
                              'pre_race_pagerank', 'shifted_rolling_ESI',
                              'jockey_win_pct', 'career_wins']].to_string())
    conn.close()
