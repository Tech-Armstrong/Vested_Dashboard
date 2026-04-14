import yfinance as yf
import json
import os
from datetime import datetime, timedelta
import pandas as pd

# ── ETF Universe ──────────────────────────────────────────────────────────────
ETFS = [
    # Broad Market
    "SPY","IVV","VOO","VTI","ITOT","SCHB","IWM","IWF","IWD","MDY","IJH","IJR",
    # NASDAQ / Growth
    "QQQ","QQQM","VUG","VONG","IWY",
    # International
    "VEA","VWO","EFA","EEM","IEFA","IEMG","VGK","EWJ","FXI","EWZ",
    # Bonds
    "AGG","BND","TLT","IEF","SHY","LQD","HYG","MUB","TIPS","VCIT",
    # Sector
    "XLK","XLF","XLV","XLE","XLY","XLP","XLI","XLB","XLU","XLRE","XLC",
    # Thematic / Factor
    "GLD","SLV","GDX","USO","DBC","VNQ","ARKK","ARKG","ARKW","ARKF",
    "ICLN","CLEAN","SOXX","SMH","IBB","XBI","JETS","BOTZ","ROBO",
    # Dividend
    "VYM","SCHD","DVY","SDY","HDV","DGRO",
    # Leveraged (informational)
    "TQQQ","SQQQ","UPRO","SPXU",
]
BENCHMARK = "SPY"

DATA_FILE = "docs/data/etf_data.json"
HISTORY_FILE = "docs/data/etf_history.json"

def pct(new, old):
    if old and old != 0:
        return round((new - old) / abs(old) * 100, 2)
    return None

def load_json(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)

def fetch_returns(ticker, end, periods):
    """Download enough history and compute returns for each period."""
    start = end - timedelta(days=periods["3Y"] + 10)
    df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                     end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                     progress=False, auto_adjust=True)
    if df.empty:
        return None, {}

    # Latest close
    price = float(df["Close"].iloc[-1])
    prev_close = float(df["Close"].iloc[-2]) if len(df) >= 2 else None
    daily_ret = pct(price, prev_close)

    returns = {}
    for label, days in periods.items():
        target = end - timedelta(days=days)
        past = df[df.index <= pd.Timestamp(target)]
        if not past.empty:
            returns[label] = pct(price, float(past["Close"].iloc[-1]))
        else:
            returns[label] = None

    return price, daily_ret, returns

def main():
    today = datetime.utcnow().date()
    periods = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095}

    existing = load_json(DATA_FILE)
    history = load_json(HISTORY_FILE)   # { "SPY": { "2024-01-02": 478.5, ... }, ... }

    snapshot = {}
    today_str = today.strftime("%Y-%m-%d")

    for ticker in ETFS:
        print(f"Fetching {ticker}...")
        try:
            result = fetch_returns(ticker, today, periods)
            if result[0] is None:
                print(f"  ⚠ No data for {ticker}")
                continue
            price, daily_ret, rets = result

            snapshot[ticker] = {
                "price": price,
                "daily": daily_ret,
                "returns": rets,
                "updated": today_str,
            }

            # Persist price in history
            if ticker not in history:
                history[ticker] = {}
            history[ticker][today_str] = price

        except Exception as e:
            print(f"  ✗ Error {ticker}: {e}")

    # Attach benchmark for comparison in dashboard
    snapshot["_benchmark"] = BENCHMARK
    snapshot["_updated"] = today_str

    save_json(DATA_FILE, snapshot)

    # Trim history older than 4 years to keep file size sane
    cutoff = (today - timedelta(days=4*365)).strftime("%Y-%m-%d")
    for tk in history:
        history[tk] = {d: v for d, v in history[tk].items() if d >= cutoff}
    save_json(HISTORY_FILE, history)

    print(f"✅ Done — {len(snapshot)-2} ETFs saved.")

if __name__ == "__main__":
    main()
