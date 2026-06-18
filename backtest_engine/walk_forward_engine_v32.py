"""
Walk-Forward Backtest Engine — v32 (CACHED)
============================================
Phase 55.4 implementation, optimized per Phase 55.4-opt.

OPTIMIZATION OVER THE FIRST DRAFT:
  Under Candidate 1 (full rebuild from 2011), a race's feature snapshot is
  IDENTICAL regardless of which test season is being run — it is always
  "replay 2011 -> that race." Snapshots are therefore season-independent
  and can be computed ONCE, cached to disk, and sliced per season.

    First draft : 9 seasons x full replay each   (~3 hours)
    This version: 1 replay -> disk cache -> slice (~35 min first build,
                  then ~5 min per dev run that only changes desk params)

  Also folds in the PER-DAY PAGERANK fidelity fix (see engine v2): PageRank
  is frozen per-day, matching v31's EquineNetworkEngineer groupby('date').

CACHE INVALIDATION:
  The cache stores feature snapshots. If a FEATURE definition changes,
  rebuild with --rebuild-cache. If only DESK parameters change (Kelly, EV,
  odds bands), the cache is still valid — reuse it (the common Tier 2 case).

SEAL PROTECTION:
  The development cache is built only THROUGH end of 2024/25. It physically
  contains no 2025/26 snapshots, so accidental seal-break during
  development is structurally impossible. The sealed run builds its own
  cache (through 2025/26) and is gated behind a typed confirmation.

Run from project root:
    python3 backtest_engine/walk_forward_engine_v32.py --mode development
    python3 backtest_engine/walk_forward_engine_v32.py --mode development --rebuild-cache
    python3 backtest_engine/walk_forward_engine_v32.py --mode single --season 2018/19
    python3 backtest_engine/walk_forward_engine_v32.py --mode sealed     # ONCE
"""

import os
import sys
import math
import pickle
import argparse
import logging
import sqlite3
import itertools
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression

_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "data_pipeline"))
from stateful_feature_engine import StatefulFeatureEngine  # noqa: E402

DB_PATH    = os.path.join(_PROJECT_ROOT, "data", "hk_racing.db")
CACHE_DIR  = os.path.join(_PROJECT_ROOT, "data", "feature_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(_PROJECT_ROOT, "data", "walk_forward_log.txt")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# =====================================================================
# LOCKED PARAMETERS (v31 spec)
# =====================================================================
MODEL_FEATURES = [
    'pre_race_elo', 'pre_race_glicko_mu', 'pre_race_glicko_rd',
    'pre_race_glicko_vol', 'pre_race_pagerank',
    'shifted_rolling_ESI', 'shifted_rolling_CSI',
    'race_ESI_pressure', 'pace_advantage',
    'jockey_win_pct', 'trainer_win_pct', 'draw',
    'days_since_last_run', 'weight_delta', 'distance_delta',
    'career_wins', 'is_turf',
]

XGB_PARAMS = dict(
    tree_method='hist', objective='rank:pairwise',
    learning_rate=0.05, max_depth=4, colsample_bytree=0.5,
    n_estimators=150, random_state=42,
)

MIN_FIELD       = 7
ANCHOR_MAX_ODDS = 5.0
LEG_MIN_ODDS    = 7.0
N_LEGS          = 4
EV_THRESHOLD    = 1.05
KELLY_MULT      = 0.05
RAKE            = 0.23
MIN_TICKET      = 10.0
MIN_BLOCK_BET   = 60.0
STARTING_BANKROLL = 100000.0

TRAIN_LOOKBACK_SEASONS = 5
DEV_SEASONS  = [f"{y}/{y+1}" for y in range(2016, 2025)]   # 2016/17..2024/25
SEALED_SEASON = "2025/26"


# =====================================================================
# SEASON / DATE HELPERS
# =====================================================================
def season_bounds(season: str):
    y0 = int(season.split("/")[0])
    return (f"{y0}-09-01", f"{y0+1}-08-31")

def train_window_bounds(test_season: str):
    y0 = int(test_season.split("/")[0])
    first = f"{y0 - TRAIN_LOOKBACK_SEASONS}/{y0 - TRAIN_LOOKBACK_SEASONS + 1}"
    last  = f"{y0 - 1}/{y0}"
    return (season_bounds(first)[0], season_bounds(last)[1])


# =====================================================================
# HARVILLE
# =====================================================================
def harville_unordered_trio(prob: dict, combo: tuple) -> float:
    total = 0.0
    for perm in itertools.permutations(combo):
        a, b, c = perm
        pa, pb, pc = prob.get(a, 0.0), prob.get(b, 0.0), prob.get(c, 0.0)
        d1 = 1.0 - pa
        d2 = 1.0 - pa - pb
        if d1 <= 0 or d2 <= 0:
            continue
        total += pa * (pb / d1) * (pc / d2)
    return total


# =====================================================================
# CONTAINERS
# =====================================================================
@dataclass
class Bet:
    race_id: str
    date_iso: str
    block_stake: float
    per_combo_stake: float
    combos: list
    won: bool = False
    realized_payout: float = 0.0
    est_block_ev: float = 0.0

@dataclass
class SeasonResult:
    season: str
    bets: list = field(default_factory=list)
    n_races: int = 0
    bankroll_curve: list = field(default_factory=list)

    def summarize(self) -> dict:
        n_bets = len(self.bets)
        n_wins = sum(1 for b in self.bets if b.won)
        staked = sum(b.block_stake for b in self.bets)
        returned = sum(b.realized_payout for b in self.bets)
        net = returned - staked
        roi = (net / staked) if staked > 0 else 0.0
        per_bet = [(b.realized_payout - b.block_stake) / b.block_stake
                   for b in self.bets if b.block_stake > 0]
        sharpe = (np.mean(per_bet) / np.std(per_bet)
                  if len(per_bet) > 1 and np.std(per_bet) > 0 else 0.0)
        mdd, peak = 0.0, -np.inf
        for bk in self.bankroll_curve:
            peak = max(peak, bk)
            if peak > 0:
                mdd = max(mdd, (peak - bk) / peak)
        avg_odds = np.mean([b.realized_payout / b.per_combo_stake
                            for b in self.bets if b.won and b.per_combo_stake > 0]) if n_wins else 0.0
        return {
            'season': self.season, 'n_races': self.n_races, 'n_bets': n_bets,
            'n_wins': n_wins, 'strike_rate': (n_wins / n_bets) if n_bets else 0.0,
            'total_staked': staked, 'total_returned': returned, 'net': net,
            'roi': roi, 'sharpe_per_bet': sharpe, 'max_drawdown': mdd,
            'avg_winning_odds': avg_odds,
        }


# =====================================================================
# FEATURE CACHE BUILDER (one chronological pass, per-day PageRank)
# =====================================================================
class FeatureCacheBuilder:
    def __init__(self, conn):
        self.conn = conn
        self.fe = StatefulFeatureEngine(conn)

    def _chrono_races_through(self, end_iso: str):
        q = """
            SELECT race_id, date_iso FROM race_metadata
            WHERE is_bettable = 1 AND date_iso <= ?
            ORDER BY date_iso, race_no
        """
        return [(rid, d) for rid, d in self.conn.execute(q, (end_iso,))]

    def build(self, end_iso: str, cache_path: str) -> pd.DataFrame:
        log.info(f"Building feature cache through {end_iso} ...")
        self.fe.reset()
        self.fe.use_daily_pr_freeze = True       # per-day PageRank (v31-faithful)

        races = self._chrono_races_through(end_iso)
        log.info(f"  {len(races):,} races to replay")

        snaps = []
        current_day = None
        for k, (rid, day) in enumerate(races):
            if day != current_day:
                # day boundary: freeze PR from graph (edges through prev day)
                self.fe.freeze_daily_pagerank()
                current_day = day
            snap = self.fe.snapshot_for(rid)
            if len(snap):
                snaps.append(snap)
            self.fe.advance_race(rid)
            if (k + 1) % 1000 == 0:
                log.info(f"  ... {k+1:,}/{len(races):,} races")

        cache = pd.concat(snaps, ignore_index=True)
        with open(cache_path, 'wb') as f:
            pickle.dump(cache, f)
        log.info(f"  cache saved: {cache_path} ({len(cache):,} rows)")
        return cache


# =====================================================================
# WALK-FORWARD ENGINE (cache-backed)
# =====================================================================
class WalkForwardEngine:
    def __init__(self, db_path=DB_PATH, end_iso=None, rebuild=False):
        self.conn = sqlite3.connect(db_path)
        # cache horizon: dev -> end of 2024/25; sealed -> end of 2025/26
        self.end_iso = end_iso or season_bounds(DEV_SEASONS[-1])[1]
        self.cache_path = os.path.join(CACHE_DIR, f"feature_cache_through_{self.end_iso}.pkl")
        self.cache = self._load_or_build(rebuild)
        # index cache by race for fast slicing
        self.cache_by_race = dict(tuple(self.cache.groupby('race_id')))

    def _load_or_build(self, rebuild):
        if (not rebuild) and os.path.exists(self.cache_path):
            log.info(f"Loading feature cache: {self.cache_path}")
            with open(self.cache_path, 'rb') as f:
                return pickle.load(f)
        builder = FeatureCacheBuilder(self.conn)
        return builder.build(self.end_iso, self.cache_path)

    # ---- clean Trio dividends (settlement) ----
    def _clean_trio_dividends(self, race_id):
        q = """
            SELECT combo, dividend FROM exotic_dividends
            WHERE race_id = ? AND pool = 'TRIO' AND is_refund = 0
              AND combo NOT LIKE '%/%'
              AND combo GLOB '*[0-9]*'
              AND combo NOT GLOB '*[A-Za-z]*'
        """
        out = []
        for combo, div in self.conn.execute(q, (race_id,)):
            try:
                nums = frozenset(int(x) for x in str(combo).split(',') if x.strip().isdigit())
                if len(nums) == 3 and div is not None:
                    out.append((nums, float(div)))
            except Exception:
                continue
        return out

    # ---- model fit (per train window) ----
    def _fit_model(self, train_df):
        df = train_df.dropna(subset=MODEL_FEATURES).copy()
        df = df[df['finish_position'].notna()].sort_values(['date_iso', 'race_id'])
        X = df[MODEL_FEATURES].astype(float)
        y = (20 - pd.to_numeric(df['finish_position'], errors='coerce').fillna(20)).clip(lower=0)
        groups = df.groupby('race_id', sort=False).size().values
        ranker = xgb.XGBRanker(**XGB_PARAMS)
        ranker.fit(X, y, group=groups)
        df['model_score'] = ranker.predict(X)
        df['is_win']   = (pd.to_numeric(df['finish_position'], errors='coerce') == 1).astype(int)
        df['is_place'] = (pd.to_numeric(df['finish_position'], errors='coerce') <= 3).astype(int)
        cal_win = LogisticRegression(solver='lbfgs', max_iter=500)
        cal_win.fit(df[['model_score']].values, df['is_win'].values)
        cal_place = LogisticRegression(solver='lbfgs', max_iter=500)
        cal_place.fit(df[['model_score']].values, df['is_place'].values)
        return ranker, cal_win, cal_place

    # ---- execution desk (Phase 53 Structural Anchor) ----
    def _execute_desk(self, race_id, snap, ranker, cal_win, cal_place, bankroll):
        df = snap.dropna(subset=MODEL_FEATURES).copy()
        if len(df) < MIN_FIELD:
            return None
        df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
        df = df.dropna(subset=['win_odds'])
        if len(df) < MIN_FIELD:
            return None

        X = df[MODEL_FEATURES].astype(float)
        df['model_score'] = ranker.predict(X)
        df['p_win_cal']   = cal_win.predict_proba(df[['model_score']].values)[:, 1]
        df['p_place_cal'] = cal_place.predict_proba(df[['model_score']].values)[:, 1]
        df['model_rank']  = df['model_score'].rank(ascending=False, method='first')

        anchor_row = df[df['model_rank'] == 1.0]
        if len(anchor_row) == 0:
            return None
        anchor = anchor_row.iloc[0]
        if anchor['win_odds'] > ANCHOR_MAX_ODDS:
            return None
        anchor_id = anchor['horse_id']

        leg_pool = df[(df['horse_id'] != anchor_id) & (df['win_odds'] >= LEG_MIN_ODDS)]
        if len(leg_pool) < N_LEGS:
            return None
        legs = leg_pool.nlargest(N_LEGS, 'p_place_cal')
        leg_ids = legs['horse_id'].tolist()

        eng_p = df.set_index('horse_id')['p_win_cal']
        eng_p = (eng_p / eng_p.sum()).to_dict()
        df['inv_odds'] = 1.0 / df['win_odds']
        pub_p = (df.set_index('horse_id')['inv_odds'] / df['inv_odds'].sum()).to_dict()

        combos = [(anchor_id, a, b) for a, b in itertools.combinations(leg_ids, 2)]

        block_hit_prob = 0.0
        synth_payouts = []
        for combo in combos:
            p_eng = harville_unordered_trio(eng_p, combo)
            p_pub = harville_unordered_trio(pub_p, combo)
            if p_pub <= 0:
                synth_payouts.append(0.0)
                continue
            synth_payouts.append((1.0 / p_pub) * (1.0 - RAKE))
            block_hit_prob += p_eng

        valid_payouts = [s for s in synth_payouts if s > 0]
        if not valid_payouts or block_hit_prob <= 0:
            return None
        avg_synth = float(np.mean(valid_payouts))
        block_ev = block_hit_prob * avg_synth
        if block_ev < EV_THRESHOLD:
            return None

        b = avg_synth - 1.0
        if b <= 0:
            return None
        f_star = (b * block_hit_prob - (1.0 - block_hit_prob)) / b
        f = max(0.0, f_star * KELLY_MULT)
        block_stake = f * bankroll
        if block_stake < MIN_BLOCK_BET:
            return None

        per_combo = max(MIN_TICKET, round((block_stake / len(combos)) / MIN_TICKET) * MIN_TICKET)
        block_stake = per_combo * len(combos)
        if block_stake > bankroll:
            return None

        # combos -> horse_no sets for settlement (horse_no is in the snapshot)
        no_map = df.set_index('horse_id')['horse_no'].to_dict()
        combo_nos = []
        for combo in combos:
            try:
                nos = frozenset(int(no_map[hid]) for hid in combo)
            except (KeyError, TypeError, ValueError):
                continue
            if len(nos) == 3:
                combo_nos.append(nos)

        return Bet(
            race_id=race_id, date_iso=str(snap['date_iso'].iloc[0]),
            block_stake=block_stake, per_combo_stake=per_combo,
            combos=combo_nos, est_block_ev=block_ev,
        )

    # ---- settlement (real dividends) ----
    def _settle(self, bet: Bet) -> Bet:
        winning = self._clean_trio_dividends(bet.race_id)
        if not winning:
            bet.won, bet.realized_payout = False, 0.0
            return bet
        payout, hit = 0.0, False
        for combo_set in bet.combos:
            for win_set, div in winning:
                if combo_set == win_set:
                    hit = True
                    payout += (bet.per_combo_stake / 10.0) * div
        bet.won, bet.realized_payout = hit, payout
        return bet

    # ---- single season (slice cache, no replay) ----
    def run_season(self, test_season: str) -> SeasonResult:
        log.info(f"=== Season {test_season} ===")
        tr_start, tr_end = train_window_bounds(test_season)
        te_start, te_end = season_bounds(test_season)
        log.info(f"  train {tr_start}->{tr_end}  test {te_start}->{te_end}")

        c = self.cache
        train_df = c[(c['date_iso'] >= tr_start) & (c['date_iso'] <= tr_end)]
        log.info(f"  train rows: {len(train_df):,} ({train_df['race_id'].nunique():,} races)")
        ranker, cal_win, cal_place = self._fit_model(train_df)

        # test races in chronological order
        test_ids = (c[(c['date_iso'] >= te_start) & (c['date_iso'] <= te_end)]
                    .drop_duplicates('race_id')
                    .sort_values(['date_iso', 'race_no'])['race_id'].tolist())

        result = SeasonResult(season=test_season)
        bankroll = STARTING_BANKROLL
        result.bankroll_curve.append(bankroll)
        for rid in test_ids:
            snap = self.cache_by_race.get(rid)
            result.n_races += 1
            if snap is None or len(snap) == 0:
                continue
            bet = self._execute_desk(rid, snap, ranker, cal_win, cal_place, bankroll)
            if bet is not None:
                bet = self._settle(bet)
                bankroll = bankroll - bet.block_stake + bet.realized_payout
                result.bets.append(bet)
                result.bankroll_curve.append(bankroll)

        s = result.summarize()
        log.info(f"  RESULT {test_season}: ROI={s['roi']*100:+.2f}% "
                 f"bets={s['n_bets']} strike={s['strike_rate']*100:.2f}% "
                 f"MDD={s['max_drawdown']*100:.1f}% avg_odds={s['avg_winning_odds']:.1f}")
        return result

    def run_development(self):
        log.info("#" * 60)
        log.info("# DEVELOPMENT RUN (Tier 2: 2016/17-2024/25). 2025/26 SEALED.")
        log.info("#" * 60)
        results = {s: self.run_season(s) for s in DEV_SEASONS}
        self._report(results)
        return results

    def run_sealed(self):
        log.info("#" * 60)
        log.info("# SEALED HOLDOUT — 2025/26 — ONCE ONLY")
        log.info("#" * 60)
        r = self.run_season(SEALED_SEASON)
        self._report({SEALED_SEASON: r})
        return r

    def _report(self, results: dict):
        rep = pd.DataFrame([r.summarize() for r in results.values()])
        log.info("\n" + "=" * 70 + "\nPER-SEASON RESULTS\n" + "=" * 70)
        log.info("\n" + rep.to_string(index=False))
        all_bets = [b for r in results.values() for b in r.bets]
        if all_bets:
            rets = np.array([(b.realized_payout - b.block_stake) / b.block_stake
                             for b in all_bets if b.block_stake > 0])
            rng = np.random.default_rng(42)
            boot = [rng.choice(rets, size=len(rets), replace=True).mean() for _ in range(1000)]
            lo, hi = np.percentile(boot, [2.5, 97.5])
            log.info("\n" + "=" * 70 + "\nPOOLED (bet-level)\n" + "=" * 70)
            log.info(f"  Total bets:      {len(rets):,}")
            log.info(f"  Pooled ROI:      {rets.mean()*100:+.2f}%")
            log.info(f"  Bootstrap 95%CI: [{lo*100:+.2f}%, {hi*100:+.2f}%]")
            log.info(f"  Strike rate:     {sum(1 for b in all_bets if b.won)/len(all_bets)*100:.2f}%")


# =====================================================================
# CLI
# =====================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=['development', 'sealed', 'single'], default='development')
    ap.add_argument('--season', default=None)
    ap.add_argument('--rebuild-cache', action='store_true',
                    help="force rebuild of the feature cache (after feature changes)")
    args = ap.parse_args()

    if args.mode == 'sealed':
        # sealed cache extends through 2025/26
        end_iso = season_bounds(SEALED_SEASON)[1]
        confirm = input("Run SEALED 2025/26 holdout? This should happen ONCE. "
                        "Type 'SEAL BREAK' to confirm: ")
        if confirm.strip() != 'SEAL BREAK':
            log.info("Aborted. Seal intact.")
            return
        eng = WalkForwardEngine(end_iso=end_iso, rebuild=args.rebuild_cache)
        eng.run_sealed()
        return

    # development / single: cache horizon = end of last dev season (2024/25)
    eng = WalkForwardEngine(rebuild=args.rebuild_cache)
    if args.mode == 'development':
        eng.run_development()
    elif args.mode == 'single':
        if not args.season:
            log.error("--mode single requires --season"); return
        if args.season == SEALED_SEASON:
            log.error("Refusing sealed season via --mode single. Use --mode sealed."); return
        r = eng.run_season(args.season)
        eng._report({args.season: r})


if __name__ == "__main__":
    main()
