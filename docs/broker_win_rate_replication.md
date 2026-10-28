# Broker Win-Rate Study — Replication Steps

Source: `broker_alpha_exploration.ipynb` §1-8 (event-study section). Engine: `broker_event_study.py` (`BrokerEventStudy` class). This doc is a runnable recipe, not a new finding — see `docs/RESEARCH_FINDINGS.md` → "Independent event-study alpha" for the verdict already reached from this method.

## What "win rate" means here

Per broker-stock pair, over days that broker **net-buys** the stock (`net_value > 0`, an "entry" day): the % of those entry days where the stock's forward return is positive, at 1d / 5d / 10d horizons. Win rate alone is descriptive — the notebook's actual alpha test is a Welch t-test of entry-day returns vs. the stock's own all-day baseline (self-baseline cancels stock-specific drift, no external index needed).

## Prerequisites

```python
from supabase import create_client
from dotenv import load_dotenv
import os
load_dotenv()
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

from bandarmology_pipeline import BandarmologyPipeline
from broker_event_study import BrokerEventStudy
```

## Step 1 — Pull data

```python
TICKERS = [
    'AADI', 'ADMR', 'ADRO', 'AMMN', 'ANTM', 'BBRI', 'BIPI', 'BNBR', 'BREN', 'BRMS',
    'BRPT', 'BUMI', 'BUVA', 'CBDK', 'CDIA', 'CUAN', 'DSSA', 'EMAS', 'ENRG', 'ESSA',
    'INCO', 'INKP', 'ITMG', 'MBMA', 'MDKA', 'MINA', 'PADI', 'PANI', 'PART', 'PGAS',
    'PSKT', 'PTRO', 'RAJA', 'RATU', 'TKIM', 'TPIA', 'VKTR',
]
START_DATE = '2025-01-01'
END_DATE = '2026-07-15'   # bump to today's date to extend the window

pipeline = BandarmologyPipeline(sb)
dfs_brok, prices_dict = pipeline.fetch_market_data(TICKERS, START_DATE, END_DATE)
```

Swap `TICKERS` for any IDX symbol list — the method is universe-agnostic. `dfs_brok`: dict of per-symbol broker DataFrames. `prices_dict`: dict of per-symbol yfinance OHLCV DataFrames.

## Step 2 — Forward returns + merge

```python
study = BrokerEventStudy()
prices_ret = study.compute_forward_returns(prices_dict)   # adds ret_1d/ret_5d/ret_10d per symbol
merged     = study.merge_broker_returns(dfs_brok, prices_ret)  # broker rows + forward-return cols
```

`ret_Nd[t] = Close[t+N]/Close[t] - 1`, close-to-close, no look-ahead (`.shift(-N)`).

## Step 3 — Win rate + entry stats

```python
df_stats = study.compute_entry_stats(merged, min_entries=15)
```

Per broker-stock: `n_entries`, `win_rate_{1d,5d,10d}`, `mean_ret`, `median_ret`, `std_ret`. Pairs with fewer than `min_entries=15` qualifying entry days → `NaN` (not zero — too thin to trust, keep out of any ranking).

## Step 4 — Alpha test (Welch t-test)

```python
df_alpha = study.test_alpha(merged, prices_ret, df_stats, min_entries=15)
```

Adds `t_stat_Nd`, `p_value_Nd`, `excess_ret_Nd` (entry-day mean return − stock's own all-day baseline mean), `has_alpha_Nd`. `has_alpha_Nd = True` only when `excess_ret > 0` AND `p_value < 0.05` AND both entry and baseline samples clear `min_entries`; `NaN` when data's too thin (never silently `False`).

## Step 5 — Leaderboard

```python
board_5d = study.leaderboard(df_alpha, horizon='5d', min_stocks=3)
```

Aggregates per broker across stocks: `n_stocks`, `n_stocks_alpha`, `avg_excess_ret`, `avg_win_rate`, `total_entries` — sorted by `avg_excess_ret` descending. Only brokers with alpha data on ≥ `min_stocks` stocks qualify. Repeat for `horizon='1d'` / `'10d'`.

## Step 6 — Save (matches notebook's `Data/` outputs)

```python
board_1d.to_csv('Data/broker_leaderboard_1d.csv', index=False)
board_5d.to_csv('Data/broker_leaderboard_5d.csv', index=False)
board_10d.to_csv('Data/broker_leaderboard_10d.csv', index=False)
df_alpha.to_csv('Data/broker_event_study_full.csv', index=False)
```

## Per-stock / per-broker lookup

To check one broker-stock pair directly instead of the aggregate leaderboard:

```python
df_alpha[df_alpha['symbol'] == 'BREN'].sort_values('excess_ret_5d', ascending=False)
```

## Known result (2025-01-02 → 2026-07-16 run, 2,118 pairs, 37 symbols)

Best universe broker: `YB` (+3.63% avg excess return, 5d). Most broker-stock pairs show **no** confirmed edge — per-stock broker alpha is real but rare. Full writeup: `broker_event_study_findings.md`.

## Reuse notes

- `compute_volume_spikes` / `identify_move_days` / `pre_event_flow_signature` / `lead_broker_frequency` (same class) run the notebook's separate "10%+ move tell-sign" study — not part of the win-rate leaderboard, don't call unless replicating that section too.
- To extend the date window: bump `END_DATE` and rerun steps 1-6; no code changes needed.
- To test a different horizon set: `BrokerEventStudy.HORIZONS = {'1d': 1, '5d': 5, '10d': 10}` is a class attribute — edit before instantiating if a new horizon is needed everywhere.

---

## Step 7 — Broker Entry Timeline (Interactive Bubble Chart)

Visualises every broker entry day as a positioned, sized, and colour-coded bubble. Implemented in `src/app/page_broker_event_study.py` (Chart 5 in the **Charts & Results** tab).

### What each bubble encodes

| Visual property | Data mapped |
|---|---|
| **X position** | Entry date (`trade_date`) |
| **Y position** | Broker code (categorical, alphabetical) |
| **Size** | Entry size in Rp Billion (`net_value / 1e9`), scaled linearly to diameter range [6 px – 40 px] |
| **Colour** | Forward return at selected horizon (`ret_Nd × 100`), clamped to ±10 % on a RdYlGn scale — red = losing entry, green = winning entry |

### Hover tooltip

Hovering any bubble shows:
```
<Broker> → <Symbol>
Date: YYYY-MM-DD
Entry Size: Rp X.XXB
Nd Return: +X.XX%
Alpha: ✅ Alpha | ❌ No alpha | —
```

The **Alpha** field reflects `has_alpha_Nd` from `test_alpha()` — `True` only when
`excess_ret > 0 AND p_value < 0.05` and both samples clear `min_entries`.

### Interactive controls

| Control | Effect |
|---|---|
| **Filter by Stock** (multiselect) | Restrict bubbles to selected symbols; blank = all symbols |
| **Filter by Broker** (multiselect) | Default = top-10 leaderboard brokers |
| **Min Entry Size (Rp B)** | Hide small entries to reduce clutter; default = 1 B |
| **Return Horizon** radio | Shared with the other Charts tab charts (1d / 5d / 10d) |
| Legend click | Show/hide individual broker traces |

### How to reproduce from `merged`

```python
horizon = '5d'
ret_col = f'ret_{horizon}'

bubble = merged[merged['net_value'] > 0].copy()
bubble['size_b']  = bubble['net_value'] / 1e9
bubble['ret_pct'] = bubble[ret_col] * 100

# Optional filters
bubble = bubble[bubble['broker'].isin(top_brokers)]
bubble = bubble[bubble['size_b'] >= 1.0]
bubble = bubble.dropna(subset=['date', 'broker', 'ret_pct'])

# Colour clamp
CLAMP = 10.0
bubble['ret_clamped'] = bubble['ret_pct'].clip(-CLAMP, CLAMP)

# Marker size normalisation [6, 40]
sz = bubble['size_b']
bubble['msize'] = 6 + 34 * (sz - sz.min()) / (sz.max() - sz.min()).clip(lower=1)
```

### Interpretation guide

- A **tall column of green bubbles** on a single date = multiple brokers entered the same stock and it moved up — confirms coordinated accumulation.
- A **large red bubble** = a big entry day that did *not* pay off at the selected horizon.
- Brokers whose bubbles are **mostly green across the timeline** match the leaderboard top — those are the ones with systematic edge.
- Sparse or missing bubbles for a broker-stock = below `min_entries` threshold; check `df_stats` for `n_entries`.

### Raw entry table

An expandable **📄 Raw Entry Table** below the chart lists every bubble's data row (Date, Broker, Symbol, Entry Size, Return, Alpha flag) for export or further analysis.
