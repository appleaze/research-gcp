import pandas as pd
import yfinance as yf
import numpy as np
import datetime
import os

from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv('g:/Dean/Saham IDX/Research-Bandarmology/.env')
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        print("Supabase URL or Key not found in environment!")
        return None
    return create_client(url, key)

def fetch_supabase_broksum(symbol):
    client = get_supabase_client()
    if not client:
        return pd.DataFrame()
        
    print(f"Fetching {symbol} from Supabase...")
    all_data = []
    offset = 0
    limit = 1000
    while True:
        resp = client.table("broker_summary").select("*") \
            .eq("symbol", symbol.upper()) \
            .range(offset, offset + limit - 1).execute()
            
        data = resp.data
        if not data:
            break
        all_data.extend(data)
        if len(data) < limit:
            break
        offset += limit
        
    df = pd.DataFrame(all_data)
    if not df.empty:
         df['Date'] = pd.to_datetime(df['trade_date'])
         df = df.sort_values('Date')
    return df

def load_data(symbol):
    df = fetch_supabase_broksum(symbol)
    if df.empty:
        print("No data returned from Supabase!")
        return df, pd.DataFrame()
        
    start_date = df['Date'].min()
    end_date = df['Date'].max() + pd.Timedelta(days=1)
    
    print(f"Fetching Price Data for {symbol}.JK from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    df_price = yf.download(f"{symbol}.JK", start=start_date, end=end_date, progress=False)
    if isinstance(df_price.columns, pd.MultiIndex):
        df_price.columns = [col[0] for col in df_price.columns]
    df_price = df_price.reset_index()
    df_price['Date'] = pd.to_datetime(df_price['Date']).dt.tz_localize(None)
    
    return df, df_price

def get_broker_classifications(df_window, df_price_window):
    all_brokers = df_window['broker'].unique()
    
    range_high = df_price_window['High'].max() if not df_price_window.empty else 0
    range_low = df_price_window['Low'].min() if not df_price_window.empty else 0
    price_range = range_high - range_low if range_high > range_low else 1

    classifications = {}
    for b in all_brokers:
        if pd.isna(b): continue
        b_df = df_window[df_window['broker'] == b]
        
        b_buy_days = len(b_df[b_df['net_value'] > 0])
        b_sell_days = len(b_df[b_df['net_value'] < 0])
        total_days = len(b_df)
        
        b_buy_val = b_df['buy_value'].sum()
        b_sell_val = b_df['sell_value'].sum()
        net_val = b_buy_val - b_sell_val
        
        # VWAP
        vwabp = (b_df['buy_value'].sum() / (b_df['buy_value'] / b_df['buy_avg_price']).replace({0:1, float('inf'):1}).sum()) if b_df['buy_value'].sum() > 0 else 0
        vwasp = (b_df['sell_value'].sum() / (b_df['sell_value'] / b_df['sell_avg_price']).replace({0:1, float('inf'):1}).sum()) if b_df['sell_value'].sum() > 0 else 0
        
        flip_ratio = min(b_buy_days, b_sell_days) / total_days if total_days > 0 else 0
        buy_pos = (vwabp - range_low) / price_range if vwabp > 0 else 0.5
        sell_pos = (vwasp - range_low) / price_range if vwasp > 0 else 0.5
        
        category = "Other"
        if total_days > 2:
            if flip_ratio >= 0.45:
                pass # Noise
            else:
                profitable = vwasp > vwabp if (vwabp > 0 and vwasp > 0) else False
                
                if net_val > 0:
                    if buy_pos < 0.5:
                        category = "SmartAccumulator"
                    else:
                        category = "RetailFOMO"
                elif net_val < 0:
                    if sell_pos >= 0.5:
                        category = "SmartDistributor"
                    else:
                        category = "RetailPanic"
                else:
                    if profitable:
                        category = "SmartSwing"
                    else:
                        category = "RetailSwing"
                    
        classifications[b] = category
    return classifications

def run_backtest(symbol="BBRI", profile_window=10, signal_window=3, strategy="short_term_swing"):
    df_brok, df_price = load_data(symbol)
    if df_brok.empty or df_price.empty:
        return
        
    timeline_dates = sorted(df_brok['Date'].unique())
    
    if len(timeline_dates) < profile_window + signal_window:
        print("Not enough data to run backtest.")
        return
        
    portfolio_cash = 100_000_000
    portfolio_shares = 0
    trade_log = []
    
    in_position = False
    shares_held_days = 0
    
    print(f"--- Starting Backtest for {symbol} | Strategy: {strategy} ---")
    print(f"Initial Capital: Rp {portfolio_cash:,.0f}")
    
    for i in range(profile_window, len(timeline_dates)):
        current_date_ts = pd.Timestamp(timeline_dates[i])
        
        start_profile_dt = timeline_dates[i - profile_window]
        df_brok_prof = df_brok[(df_brok['Date'] >= start_profile_dt) & (df_brok['Date'] <= current_date_ts)]
        df_price_prof = df_price[(df_price['Date'] >= start_profile_dt) & (df_price['Date'] <= current_date_ts)]
        
        broker_classes = get_broker_classifications(df_brok_prof, df_price_prof)
        smart_brokers = [b for b, cat in broker_classes.items() if cat in ['SmartAccumulator', 'SmartDistributor', 'SmartSwing']]
        retail_brokers = [b for b, cat in broker_classes.items() if cat in ['RetailFOMO', 'RetailPanic', 'RetailSwing']]
        
        start_signal_dt = timeline_dates[i - signal_window]
        df_brok_sig = df_brok[(df_brok['Date'] >= start_signal_dt) & (df_brok['Date'] <= current_date_ts)]
        
        sm_net = df_brok_sig[df_brok_sig['broker'].isin(smart_brokers)]['net_value'].sum()
        rt_net = df_brok_sig[df_brok_sig['broker'].isin(retail_brokers)]['net_value'].sum()
        
        current_price_row = df_price[df_price['Date'] == current_date_ts]
        if current_price_row.empty: continue
        close_px = current_price_row['Close'].values[0]
        
        # We need the T-3 price for the swing setup
        t_minus_3_date = pd.Timestamp(timeline_dates[i - 3]) if i >= 3 else start_profile_dt
        t_minus_3_row = df_price[df_price['Date'] == t_minus_3_date]
        close_px_t3 = t_minus_3_row['Close'].values[0] if not t_minus_3_row.empty else close_px
        
        # Trading Rules
        if not in_position:
            buy_signal = False
            
            if strategy == "long_term_divergence":
                if sm_net > 0 and rt_net < 0:
                    buy_signal = True
            elif strategy == "short_term_swing":
                # Price pulled back over last 3 days AND Smart Money accumulated
                if close_px < close_px_t3 and sm_net > 0 and rt_net < 0:
                    buy_signal = True
                    
            if buy_signal:
                shares_to_buy = portfolio_cash // close_px
                fee = shares_to_buy * close_px * 0.0015
                if portfolio_cash >= (shares_to_buy * close_px) + fee and shares_to_buy > 0:
                    portfolio_cash -= (shares_to_buy * close_px) + fee
                    portfolio_shares += shares_to_buy
                    in_position = True
                    shares_held_days = 0
                    trade_log.append({'Action': 'BUY', 'Date': current_date_ts, 'Price': close_px, 'Shares': shares_to_buy})
                    print(f"[{current_date_ts.strftime('%Y-%m-%d')}] BUY Signal | Price: {close_px:,.0f} | SM Net: {sm_net/1e9:,.0f}B")
        else:
            shares_held_days += 1
            last_buy_price = trade_log[-1]['Price']
            loss_pct = (close_px - last_buy_price) / last_buy_price
            
            sell_signal = False
            reason = ""
            
            if strategy == "long_term_divergence":
                if (sm_net < 0 and rt_net > 0) or loss_pct < -0.05:
                    sell_signal = True
                    reason = "Stop Loss" if loss_pct < -0.05 else "Distribution Signal"
            elif strategy == "short_term_swing":
                # Exit gracefully on day 4 or stop loss or massive distribution
                if shares_held_days >= 4:
                    sell_signal = True
                    reason = "Time Exit (4 Days)"
                elif loss_pct < -0.05:
                    sell_signal = True
                    reason = "Stop Loss (-5%)"
                elif sm_net < -50e9: # Violent sudden dumping
                    sell_signal = True
                    reason = "Sudden SM Dump"
            
            if sell_signal:
                sale_val = portfolio_shares * close_px
                fee = sale_val * 0.0025
                portfolio_cash += sale_val - fee
                profit = (sale_val - fee) - (portfolio_shares * last_buy_price * 1.0015)
                print(f"[{current_date_ts.strftime('%Y-%m-%d')}] SELL ({reason}) | Price: {close_px:,.0f} | P/L: {profit:,.0f} | Held: {shares_held_days} days")
                trade_log.append({'Action': 'SELL', 'Date': current_date_ts, 'Price': close_px, 'Shares': portfolio_shares, 'PnL': profit})
                portfolio_shares = 0
                in_position = False
                shares_held_days = 0
                
    if in_position:
        final_px = df_price['Close'].iloc[-1]
        sale_val = portfolio_shares * final_px
        fee = sale_val * 0.0025
        portfolio_cash += sale_val - fee
        profit = (sale_val - fee) - (portfolio_shares * trade_log[-1]['Price'] * 1.0015)
        print(f"[END OF DATA] Auto-Closing positions at {final_px:,.0f}")
        trade_log.append({'Action': 'SELL', 'Date': df_price['Date'].iloc[-1], 'Price': final_px, 'Shares': portfolio_shares, 'PnL': profit})

    final_equity = portfolio_cash
    b_and_h_shares = 100_000_000 // df_price['Close'].iloc[0]
    b_and_h_val = b_and_h_shares * df_price['Close'].iloc[-1]
    
    print("\n=== Backtest Summary ===")
    print(f"Starting Capital : Rp 100,000,000")
    print(f"Final Capital    : Rp {final_equity:,.0f}")
    print(f"Strategy Return  : {((final_equity/100_000_000)-1)*100:.2f}%")
    print(f"Buy & Hold Return: {((b_and_h_val/100_000_000)-1)*100:.2f}%")
    
    wins = len([t for t in trade_log if t['Action'] == 'SELL' and t['PnL'] > 0])
    losses = len([t for t in trade_log if t['Action'] == 'SELL' and t['PnL'] <= 0])
    total_trades = wins + losses
    win_rate = wins / total_trades if total_trades > 0 else 0
    print(f"Total Trades     : {total_trades}")
    print(f"Win Rate         : {win_rate*100:.1f}%")

if __name__ == "__main__":
    # We use a 20-day profiling window to figure out the active players.
    # We use a 3-day short-term signal to look for fast accumulation setups.
    run_backtest("BBRI", profile_window=20, signal_window=3, strategy="short_term_swing")
