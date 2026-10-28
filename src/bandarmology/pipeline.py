"""
pipeline.py
-----------
Data-fetch layer for broker event-study workflows backed by Google BigQuery and yfinance.
"""

from __future__ import annotations

import os
from typing import Dict, List, Tuple, Optional
import pandas as pd
import yfinance as yf
from google.cloud import bigquery

from bandarmology.clients.bigquery import BigQueryClient


class BandarmologyPipeline:
    """Pulls broker-flow data from BigQuery and OHLCV price data from yfinance.

    Parameters
    ----------
    bq_client:
        An instance of ``BigQueryClient`` or ``google.cloud.bigquery.Client``.
    dataset:
        BigQuery dataset name containing the ``broker_summary`` table (default: idx_bandarmology).
    """

    TABLE_BROKER = "broker_summary"
    BROKER_COLS = "trade_date, symbol, broker, net_value, net_lot, buy_value, sell_value, buy_lot, sell_lot, buy_avg_price, sell_avg_price"

    def __init__(self, bq_client: Optional[BigQueryClient] = None, dataset: Optional[str] = None):
        if bq_client is None:
            self._bq = BigQueryClient(dataset_id=dataset)
        else:
            self._bq = bq_client
        self.dataset = dataset or self._bq.dataset_id

    def fetch_market_data(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
    ) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
        """Fetch broker data + OHLCV prices for a list of IDX symbols.

        Parameters
        ----------
        tickers:
            List of IDX ticker symbols without the ``.JK`` suffix, e.g. ``['BBRI', 'BREN', 'ADRO']``.
        start_date, end_date:
            ISO date strings (``'YYYY-MM-DD'``), inclusive.

        Returns
        -------
        dfs_brok : dict[symbol -> DataFrame]
            Per-symbol broker_summary DataFrames.
        prices_dict : dict[symbol -> DataFrame]
            Per-symbol yfinance OHLCV DataFrames (tz-naive, date index).
        """
        dfs_brok = self._fetch_broker_data(tickers, start_date, end_date)
        prices_dict = self._fetch_price_data(tickers, start_date, end_date)
        return dfs_brok, prices_dict

    def _fetch_broker_data(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, pd.DataFrame]:
        """Vectorized BigQuery fetch for all requested tickers within date range."""
        if not tickers:
            return {}

        clean_tickers = [t.upper().strip() for t in tickers]
        query = f"""
            SELECT {self.BROKER_COLS}
            FROM `{self._bq.project_id}.{self.dataset}.{self.TABLE_BROKER}`
            WHERE symbol IN UNNEST(@tickers)
              AND trade_date BETWEEN @start_date AND @end_date
            ORDER BY symbol, trade_date ASC
        """
        params = [
            bigquery.ArrayQueryParameter("tickers", "STRING", clean_tickers),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]

        df_all = self._bq.query_to_dataframe(query, params=params)

        result: Dict[str, pd.DataFrame] = {}
        for sym in clean_tickers:
            if not df_all.empty:
                sym_df = df_all[df_all["symbol"] == sym].copy()
                if not sym_df.empty:
                    sym_df["trade_date"] = pd.to_datetime(sym_df["trade_date"]).dt.strftime("%Y-%m-%d")
                    for col in ["net_value", "net_lot", "buy_value", "sell_value",
                                "buy_lot", "sell_lot", "buy_avg_price", "sell_avg_price"]:
                        if col in sym_df.columns:
                            sym_df[col] = pd.to_numeric(sym_df[col], errors="coerce").fillna(0)
                    result[sym] = sym_df.reset_index(drop=True)
                    continue
            result[sym] = pd.DataFrame()

        return result

    def _fetch_price_data(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, pd.DataFrame]:
        """Batch yfinance download, returns tz-naive DataFrames keyed by symbol."""
        yf_end = (pd.Timestamp(end_date) + pd.Timedelta(days=14)).strftime("%Y-%m-%d")
        yf_tickers = [f"{s}.JK" for s in tickers]

        raw = yf.download(
            yf_tickers,
            start=start_date,
            end=yf_end,
            group_by="ticker",
            progress=False,
            auto_adjust=True,
        )

        result: Dict[str, pd.DataFrame] = {}
        for sym in tickers:
            yf_sym = f"{sym}.JK"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if yf_sym not in raw.columns.get_level_values(0):
                        result[sym] = pd.DataFrame()
                        continue
                    df = raw[yf_sym].copy()
                else:
                    df = raw.copy()

                df = df.dropna(subset=["Close"])
                df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
                result[sym] = df
            except Exception:
                result[sym] = pd.DataFrame()

        return result
