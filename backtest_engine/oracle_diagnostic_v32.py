"""
Oracle Signal Diagnostic — v32
================================
Phase 55.5 pre-step.

Strips away the ENTIRE execution desk (EV trigger, Kelly, longshot legs,
Harville, settlement) and measures the RAW predictive signal of the model's
horse ranking, compared head-to-head against the public (odds) ranking.

The question this answers:
  Is the -21.66% backtest a DESK problem (model ranks well, bet structure
  too expensive) or a SIGNAL problem (model can't out-predict the market)?

  - If the model out-ranks the public (esp. wins when it DIVERGES from the
    public), the signal is real -> the -21.66% is a desk/pool problem ->
    pool change / selectivity are worth pursuing.
  - If the model can't beat the public ranking, the signal is the problem ->
    no desk engineering will help -> pivot to features or accept market
    efficiency.

Uses the SAME walk-forward cache and the SAME per-season XGBRanker fit as
the real engine, so it is apples-to-apples and leak-free. No calibration is
needed: rank order is invariant to monotonic Platt scaling.

Run from project root:
    python3 backtest_engine/oracle_diagnostic_v32.py
"""

import os
import sys
import logging

import numpy as np
import pandas as pd
import xgboost as xgb

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _SCRIPT_DIR)
from walk_forward_engine_v32 import (   # noqa: E402
    WalkForwardEngine, MODEL_FEATURES, XGB_PARAMS, MIN_FIELD,
    DEV_SEASONS, season_bounds, train_window_bounds,
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def fit_ranker(train_df: pd.DataFrame) -> xgb.XGBRanker:
    """Same XGBRanker spec the real engine uses (no calibrators needed)."""
    df = train_df.dropna(subset=MODEL_FEATURES).copy()
    df = df[df['finish_position'].notna()].sort_values(['date_iso', 'race_id'])
    X = df[MODEL_FEATURES].astype(float)
    y = (20 - pd.to_numeric(df['finish_position'], errors='coerce').fillna(20)).clip(lower=0)
    groups = df.groupby('race_id', sort=False).size().values
    r = xgb.XGBRanker(**XGB_PARAMS)
    r.fit(X, y, group=groups)
    return r


class OracleDiagnostic:
    def __init__(self):
        self.eng = WalkForwardEngine()       # loads the cache
        self.cache = self.eng.cache

    def run_season(self, season: str) -> pd.DataFrame:
        tr_start, tr_end = train_window_bounds(season)
        te_start, te_end = season_bounds(season)
        c = self.cache

        train_df = c[(c['date_iso'] >= tr_start) & (c['date_iso'] <= tr_end)]
        ranker = fit_ranker(train_df)

        test = c[(c['date_iso'] >= te_start) & (c['date_iso'] <= te_end)].copy()

        recs = []
        for rid, g in test.groupby('race_id'):
            g = g.dropna(subset=MODEL_FEATURES).copy()
            g['win_odds'] = pd.to_numeric(g['win_odds'], errors='coerce')
            g['fp'] = pd.to_numeric(g['finish_position'], errors='coerce')
            g = g.dropna(subset=['win_odds', 'fp'])
            if len(g) < MIN_FIELD:
                continue

            g['ms'] = ranker.predict(g[MODEL_FEATURES].astype(float))

            g_model = g.sort_values('ms', ascending=False)
            g_pub   = g.sort_values('win_odds', ascending=True)
            g_act   = g.sort_values('fp', ascending=True)

            model_top1  = g_model.iloc[0]['horse_id']
            public_top1 = g_pub.iloc[0]['horse_id']
            model_top3  = set(g_model.iloc[:3]['horse_id'])
            public_top3 = set(g_pub.iloc[:3]['horse_id'])
            actual_top3 = set(g_act.iloc[:3]['horse_id'])
            actual_winner = g_act.iloc[0]['horse_id']
            fp_map = dict(zip(g['horse_id'], g['fp']))

            diverge = (model_top1 != public_top1)
            recs.append({
                'm_top1_win':   int(model_top1 == actual_winner),
                'p_top1_win':   int(public_top1 == actual_winner),
                'm_top1_place': int(model_top1 in actual_top3),
                'p_top1_place': int(public_top1 in actual_top3),
                'm_trio_exact': int(model_top3 == actual_top3),
                'p_trio_exact': int(public_top3 == actual_top3),
                'm_overlap':    len(model_top3 & actual_top3),
                'p_overlap':    len(public_top3 & actual_top3),
                'diverge':      int(diverge),
                # in divergence races only:
                'div_model_win': int(model_top1 == actual_winner) if diverge else np.nan,
                'div_public_win': int(public_top1 == actual_winner) if diverge else np.nan,
                'div_model_beats_public': (int(fp_map[model_top1] < fp_map[public_top1])
                                           if diverge else np.nan),
            })
        return pd.DataFrame(recs)

    def run(self):
        per_season = []
        all_recs = []
        for s in DEV_SEASONS:
            df = self.run_season(s)
            all_recs.append(df)
            per_season.append({
                'season': s,
                'races': len(df),
                'm_top1_win': df['m_top1_win'].mean(),
                'p_top1_win': df['p_top1_win'].mean(),
                'm_trio_exact': df['m_trio_exact'].mean(),
                'p_trio_exact': df['p_trio_exact'].mean(),
                'diverge_rate': df['diverge'].mean(),
                'div_m_beats_p': df['div_model_beats_public'].mean(),
            })
            log.info(f"  {s}: races={len(df)} "
                     f"model_top1_win={df['m_top1_win'].mean()*100:.1f}% "
                     f"public_top1_win={df['p_top1_win'].mean()*100:.1f}% "
                     f"div_m_beats_p={df['div_model_beats_public'].mean()*100:.1f}%")

        full = pd.concat(all_recs, ignore_index=True)
        self._report(full, pd.DataFrame(per_season))

    def _report(self, full: pd.DataFrame, per_season: pd.DataFrame):
        n = len(full)
        log.info("\n" + "=" * 72)
        log.info("PER-SEASON")
        log.info("=" * 72)
        log.info("\n" + per_season.to_string(index=False))

        log.info("\n" + "=" * 72)
        log.info(f"POOLED SIGNAL METRICS  (n = {n:,} races, field >= {MIN_FIELD})")
        log.info("=" * 72)

        def line(label, m, p):
            edge = m - p
            flag = "MODEL+" if edge > 0 else ("PUBLIC+" if edge < 0 else "tie")
            log.info(f"  {label:<34} model={m*100:6.2f}%   public={p*100:6.2f}%   "
                     f"diff={edge*100:+6.2f}pts [{flag}]")

        line("Top-1 win rate",        full['m_top1_win'].mean(),   full['p_top1_win'].mean())
        line("Top-1 place (top-3) rate", full['m_top1_place'].mean(), full['p_top1_place'].mean())
        line("Trio exact hit rate",   full['m_trio_exact'].mean(), full['p_trio_exact'].mean())
        log.info(f"  {'Avg top-3 overlap (0-3)':<34} "
                 f"model={full['m_overlap'].mean():6.3f}    public={full['p_overlap'].mean():6.3f}    "
                 f"diff={full['m_overlap'].mean()-full['p_overlap'].mean():+6.3f}")

        # ---- The crux: divergence head-to-head ----
        div = full[full['diverge'] == 1]
        log.info("\n" + "-" * 72)
        log.info(f"DIVERGENCE TEST  (model top-1 != public top-1)")
        log.info("-" * 72)
        log.info(f"  Divergence rate:            {full['diverge'].mean()*100:.1f}% "
                 f"({len(div):,} of {n:,} races)")
        if len(div) > 0:
            dm = div['div_model_win'].mean()
            dp = div['div_public_win'].mean()
            h2h = div['div_model_beats_public'].mean()
            log.info(f"  In divergence races:")
            log.info(f"    Model pick win rate:      {dm*100:.2f}%")
            log.info(f"    Public pick win rate:     {dp*100:.2f}%")
            log.info(f"    Model beats public (H2H): {h2h*100:.2f}%   (>50% = edge)")

            # bootstrap CI on H2H
            vals = div['div_model_beats_public'].dropna().values
            rng = np.random.default_rng(42)
            boot = [rng.choice(vals, size=len(vals), replace=True).mean()
                    for _ in range(2000)]
            lo, hi = np.percentile(boot, [2.5, 97.5])
            log.info(f"    H2H bootstrap 95% CI:     [{lo*100:.2f}%, {hi*100:.2f}%]")

            log.info("\n" + "=" * 72)
            log.info("VERDICT")
            log.info("=" * 72)
            if lo > 0.5:
                log.info("  EDGE DETECTED: when the model diverges from the public, it")
                log.info("  beats the market significantly more than half the time.")
                log.info("  The signal is REAL. The -21.66% is a DESK/POOL problem.")
                log.info("  -> Pursue pool change (Quinella/QP) and selectivity.")
            elif hi < 0.5:
                log.info("  NO EDGE (negative): the model's disagreements with the market")
                log.info("  are worse than coin-flips. The market out-predicts the model.")
                log.info("  -> Desk engineering cannot fix this. Pivot to features, or")
                log.info("     accept HKJC Trio-market efficiency for this feature set.")
            else:
                log.info("  INCONCLUSIVE: H2H CI straddles 50%. The model neither clearly")
                log.info("  beats nor clearly loses to the market when it diverges.")
                log.info("  -> Weak/no exploitable edge in top-pick divergence. Examine")
                log.info("     whether edge exists elsewhere (e.g. place-pool ranking).")


if __name__ == "__main__":
    OracleDiagnostic().run()
