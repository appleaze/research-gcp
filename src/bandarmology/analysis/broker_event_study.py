"""
broker_event_study.py
---------------------
Engine for the broker win-rate / event-study analysis described in
broker_win_rate_replication.md.

Quickstart (Steps 2-5 of the replication recipe)::

    from broker_event_study import BrokerEventStudy

    study      = BrokerEventStudy()
    prices_ret = study.compute_forward_returns(prices_dict)
    merged     = study.merge_broker_returns(dfs_brok, prices_ret)
    df_stats   = study.compute_entry_stats(merged, min_entries=15)
    df_alpha   = study.test_alpha(merged, prices_ret, df_stats, min_entries=15)
    board_5d   = study.leaderboard(df_alpha, horizon='5d', min_stocks=3)
"""

from __future__ import annotations

import warnings
from typing import Dict

import numpy as np
import pandas as pd
from scipy import stats


class BrokerEventStudy:
    """Win-rate and alpha analysis for broker-stock entry days.

    Class attributes
    ----------------
    HORIZONS : dict[str, int]
        Maps horizon label to number of calendar days used in
        ``ret_Nd = Close[t+N] / Close[t] - 1``.
        Edit *before* instantiation to add or remove horizons globally.
    """

    HORIZONS: Dict[str, int] = {"1d": 1, "5d": 5, "10d": 10}

    # ------------------------------------------------------------------
    # Step 2 — Forward returns
    # ------------------------------------------------------------------

    def compute_forward_returns(
        self,
        prices_dict: Dict[str, pd.DataFrame],
    ) -> Dict[str, pd.DataFrame]:
        """Add ``ret_Nd`` columns to each per-symbol price DataFrame.

        ``ret_Nd[t] = Close[t+N] / Close[t] - 1``  (close-to-close,
        no look-ahead — implemented via ``shift(-N)``).

        Parameters
        ----------
        prices_dict:
            Output of ``BandarmologyPipeline.fetch_market_data``.

        Returns
        -------
        prices_ret : dict[symbol -> DataFrame]
            Same structure as input, each DataFrame augmented with
            columns ``ret_1d``, ``ret_5d``, ``ret_10d`` (or whatever
            HORIZONS specifies).  Rows at the tail that lack a future
            close become ``NaN``.
        """
        prices_ret: Dict[str, pd.DataFrame] = {}
        for sym, df in prices_dict.items():
            if df.empty:
                prices_ret[sym] = df
                continue
            out = df.copy()
            close = out["Close"]
            for label, n in self.HORIZONS.items():
                out[f"ret_{label}"] = close.shift(-n) / close - 1
            prices_ret[sym] = out
        return prices_ret

    # ------------------------------------------------------------------
    # Step 2 cont. — Merge broker rows with forward returns
    # ------------------------------------------------------------------

    def merge_broker_returns(
        self,
        dfs_brok: Dict[str, pd.DataFrame],
        prices_ret: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Join broker rows with forward-return columns on (symbol, trade_date).

        Parameters
        ----------
        dfs_brok:
            Per-symbol broker DataFrames from the pipeline.
        prices_ret:
            Output of :meth:`compute_forward_returns`.

        Returns
        -------
        merged : DataFrame
            Flat table with all symbols and all brokers.  Columns include
            the original broker columns plus ``ret_1d``, ``ret_5d``,
            ``ret_10d``, and a normalised ``date`` column (tz-naive date).
        """
        pieces = []
        ret_cols = [f"ret_{h}" for h in self.HORIZONS]

        for sym in dfs_brok:
            brok_df = dfs_brok.get(sym, pd.DataFrame())
            price_df = prices_ret.get(sym, pd.DataFrame())

            if brok_df.empty:
                continue

            brok = brok_df.copy()
            brok["date"] = pd.to_datetime(brok["trade_date"]).dt.normalize()

            if price_df.empty or not any(c in price_df.columns for c in ret_cols):
                for col in ret_cols:
                    brok[col] = np.nan
                pieces.append(brok)
                continue

            # Price index is already date-normalised (from pipeline)
            price_sub = price_df[ret_cols].copy()
            price_sub.index = pd.to_datetime(price_sub.index).normalize()
            price_sub = price_sub.reset_index().rename(columns={"index": "date", "Date": "date"})
            # Some yfinance builds name the index 'Date'
            if "Date" in price_sub.columns and "date" not in price_sub.columns:
                price_sub = price_sub.rename(columns={"Date": "date"})

            merged_sym = brok.merge(price_sub, on="date", how="left")
            pieces.append(merged_sym)

        if not pieces:
            return pd.DataFrame()

        merged = pd.concat(pieces, ignore_index=True)
        return merged

    # ------------------------------------------------------------------
    # Step 3 — Win rate + entry stats
    # ------------------------------------------------------------------

    def compute_entry_stats(
        self,
        merged: pd.DataFrame,
        min_entries: int = 15,
    ) -> pd.DataFrame:
        """Compute per broker-stock win rates and summary statistics.

        An *entry day* is any day where ``net_value > 0`` for that
        broker-stock pair.

        Parameters
        ----------
        merged:
            Output of :meth:`merge_broker_returns`.
        min_entries:
            Pairs with fewer qualifying entry days get ``NaN`` statistics
            (kept in the output so they don't silently appear as zero).

        Returns
        -------
        df_stats : DataFrame
            One row per (broker, symbol) pair.  Columns:

            * ``n_entries``
            * ``win_rate_1d``, ``win_rate_5d``, ``win_rate_10d``
            * ``mean_ret``, ``median_ret``, ``std_ret``  (based on ret_5d)
        """
        ret_cols = {h: f"ret_{h}" for h in self.HORIZONS}
        entries = merged[merged["net_value"] > 0].copy()

        records = []
        for (broker, symbol), grp in entries.groupby(["broker", "symbol"]):
            n = len(grp)
            row: dict = {"broker": broker, "symbol": symbol, "n_entries": n}

            if n < min_entries:
                for h in self.HORIZONS:
                    row[f"win_rate_{h}"] = np.nan
                row.update({"mean_ret": np.nan, "median_ret": np.nan, "std_ret": np.nan})
            else:
                for h, col in ret_cols.items():
                    valid = grp[col].dropna()
                    row[f"win_rate_{h}"] = (valid > 0).sum() / len(valid) if len(valid) > 0 else np.nan

                r5 = grp[ret_cols["5d"]].dropna()
                row["mean_ret"]   = r5.mean()   if len(r5) > 0 else np.nan
                row["median_ret"] = r5.median() if len(r5) > 0 else np.nan
                row["std_ret"]    = r5.std()    if len(r5) > 1 else np.nan

            records.append(row)

        if not records:
            return pd.DataFrame()

        df_stats = pd.DataFrame(records)
        return df_stats

    # ------------------------------------------------------------------
    # Step 4 — Alpha test (Welch t-test)
    # ------------------------------------------------------------------

    def test_alpha(
        self,
        merged: pd.DataFrame,
        prices_ret: Dict[str, pd.DataFrame],
        df_stats: pd.DataFrame,
        min_entries: int = 15,
    ) -> pd.DataFrame:
        """Run Welch t-tests: entry-day returns vs. stock's own all-day baseline.

        The self-baseline cancels stock-specific drift without requiring an
        external index.

        Parameters
        ----------
        merged:
            Output of :meth:`merge_broker_returns`.
        prices_ret:
            Output of :meth:`compute_forward_returns`; used to build the
            per-symbol baseline distribution.
        df_stats:
            Output of :meth:`compute_entry_stats`.
        min_entries:
            Minimum entries required in *both* the entry sample and the
            stock's own baseline to attempt the t-test.  When either
            sample is too thin, ``has_alpha_Nd`` is ``NaN`` (never
            silently ``False``).

        Returns
        -------
        df_alpha : DataFrame
            ``df_stats`` extended with, per horizon ``Nd``:

            * ``t_stat_Nd``     — Welch t-statistic
            * ``p_value_Nd``    — two-tailed p-value
            * ``excess_ret_Nd`` — entry mean − stock baseline mean
            * ``has_alpha_Nd``  — True only when excess_ret > 0
                                  AND p_value < 0.05 AND both n ≥ min_entries
        """
        ret_cols = {h: f"ret_{h}" for h in self.HORIZONS}

        # Build per-symbol baseline (all trading days with a valid return)
        baselines: Dict[str, Dict[str, pd.Series]] = {}
        for sym, df in prices_ret.items():
            if df.empty:
                continue
            baselines[sym] = {}
            for h, col in ret_cols.items():
                if col in df.columns:
                    baselines[sym][h] = df[col].dropna()

        entries = merged[merged["net_value"] > 0].copy()

        df_alpha = df_stats.copy()

        for h, col in ret_cols.items():
            t_stats, p_vals, excess_rets, has_alphas = [], [], [], []

            for _, srow in df_alpha.iterrows():
                broker = srow["broker"]
                symbol = srow["symbol"]
                n_ent  = srow["n_entries"]

                sym_baseline = baselines.get(symbol, {}).get(h, pd.Series(dtype=float))
                n_base = len(sym_baseline)

                grp = entries[
                    (entries["broker"] == broker) & (entries["symbol"] == symbol)
                ][col].dropna()
                n_grp = len(grp)

                if n_grp < min_entries or n_base < min_entries:
                    t_stats.append(np.nan)
                    p_vals.append(np.nan)
                    excess_rets.append(np.nan)
                    has_alphas.append(np.nan)
                    continue

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    t_stat, p_val = stats.ttest_ind(
                        grp, sym_baseline, equal_var=False
                    )

                excess = grp.mean() - sym_baseline.mean()
                has_alpha = (
                    True if (excess > 0 and p_val < 0.05) else False
                )

                t_stats.append(t_stat)
                p_vals.append(p_val)
                excess_rets.append(excess)
                has_alphas.append(has_alpha)

            df_alpha[f"t_stat_{h}"]    = t_stats
            df_alpha[f"p_value_{h}"]   = p_vals
            df_alpha[f"excess_ret_{h}"] = excess_rets
            # NaN rows should remain NaN, not False
            df_alpha[f"has_alpha_{h}"] = pd.array(has_alphas, dtype=object)

        return df_alpha

    # ------------------------------------------------------------------
    # Step 5 — Leaderboard
    # ------------------------------------------------------------------

    def leaderboard(
        self,
        df_alpha: pd.DataFrame,
        horizon: str = "5d",
        min_stocks: int = 3,
    ) -> pd.DataFrame:
        """Aggregate per-broker alpha stats across stocks.

        Parameters
        ----------
        df_alpha:
            Output of :meth:`test_alpha`.
        horizon:
            One of the keys in ``HORIZONS`` (``'1d'``, ``'5d'``, ``'10d'``).
        min_stocks:
            Brokers with alpha data on fewer than this many stocks are excluded.

        Returns
        -------
        board : DataFrame
            Columns: ``broker``, ``n_stocks``, ``n_stocks_alpha``,
            ``avg_excess_ret``, ``avg_win_rate``, ``total_entries``.
            Sorted by ``avg_excess_ret`` descending.
        """
        if horizon not in self.HORIZONS:
            raise ValueError(f"horizon must be one of {list(self.HORIZONS.keys())}")

        exc_col   = f"excess_ret_{horizon}"
        wr_col    = f"win_rate_{horizon}"
        alpha_col = f"has_alpha_{horizon}"

        # Only keep rows where alpha data is non-NaN
        valid = df_alpha.dropna(subset=[exc_col]).copy()

        records = []
        for broker, grp in valid.groupby("broker"):
            n_stocks = len(grp)
            if n_stocks < min_stocks:
                continue

            n_alpha = int(grp[alpha_col].eq(True).sum()) if alpha_col in grp.columns else 0

            records.append({
                "broker":         broker,
                "n_stocks":       n_stocks,
                "n_stocks_alpha": n_alpha,
                "avg_excess_ret": grp[exc_col].mean(),
                "avg_win_rate":   grp[wr_col].mean() if wr_col in grp.columns else np.nan,
                "total_entries":  int(grp["n_entries"].sum()),
            })

        if not records:
            return pd.DataFrame(columns=[
                "broker", "n_stocks", "n_stocks_alpha",
                "avg_excess_ret", "avg_win_rate", "total_entries"
            ])

        board = pd.DataFrame(records).sort_values("avg_excess_ret", ascending=False).reset_index(drop=True)
        return board

    # ------------------------------------------------------------------
    # Stubs — "10%+ move tell-sign" study (separate notebook section)
    # ------------------------------------------------------------------

    def compute_volume_spikes(self, prices_dict, threshold: float = 2.0):
        """Identify days where volume exceeds ``threshold × rolling average``.

        Belongs to the "10%+ move tell-sign" section of the notebook,
        *not* part of the win-rate leaderboard workflow.
        Raise ``NotImplementedError`` until implemented.
        """
        raise NotImplementedError(
            "compute_volume_spikes is part of the move-day study (not win-rate). "
            "Implement separately if replicating that notebook section."
        )

    def identify_move_days(self, prices_dict, move_threshold: float = 0.10):
        """Find days with price moves ≥ ``move_threshold`` (absolute value).

        See :meth:`compute_volume_spikes` docstring.
        """
        raise NotImplementedError(
            "identify_move_days is part of the move-day study (not win-rate)."
        )

    def pre_event_flow_signature(self, merged, move_days, lookback: int = 5):
        """Analyse broker flow in the ``lookback`` days before each move day.

        See :meth:`compute_volume_spikes` docstring.
        """
        raise NotImplementedError(
            "pre_event_flow_signature is part of the move-day study (not win-rate)."
        )

    def lead_broker_frequency(self, pre_event_df):
        """Count how often each broker leads before a big move.

        See :meth:`compute_volume_spikes` docstring.
        """
        raise NotImplementedError(
            "lead_broker_frequency is part of the move-day study (not win-rate)."
        )
