
import yfinance as yf
import json
import os
from datetime import datetime, timedelta
import pandas as pd

ETFS = [
    "SPY","IVV","VOO","VTI","ITOT","SCHB","IWM","IWF","IWD","MDY","IJH","IJR",
    "QQQ","QQQM","VUG","VONG","IWY",
    "VEA","VWO","EFA","EEM","IEFA","IEMG","VGK","EWJ","FXI","EWZ",
    "AGG","BND","TLT","IEF","SHY","LQD","HYG","MUB","VCIT",
    "XLK","XLF","XLV","XLE","XLY","XLP","XLI","XLB","XLU","XLRE","XLC",
    "GLD","SLV","GDX","USO","DBC","VNQ","ARKK","ARKG","ARKW","ARKF",
    "ICLN","SOXX","SMH","IBB","XBI","JETS","BOTZ",
    "VYM","SCHD","DVY","SDY","HDV","DGRO",
    "TQQQ","SQQQ","UPRO","SPXU",
]
BENCHMARK = "SPY"

DATA_FILE  = "docs/data/etf_data.json"
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

def fetch_ticker_data(ticker, periods):
    """
    Download ~3.5 years of data. Works even on holidays/weekends because
    we always take the LAST available row rather than today's date.
    """
    end   = datetime.utcnow().date()
    start = end - timedelta(days=periods["3Y"] + 30)   # extra buffer

    df = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
        progress=False,
        auto_adjust=True,
    )

    if df.empty:
        return None

    # ── Use last available trading day (handles holidays & weekends) ──
    price      = float(df["Close"].iloc[-1])
    last_date  = df.index[-1].date().strftime("%Y-%m-%d")
    prev_close = float(df["Close"].iloc[-2]) if len(df) >= 2 else None
    daily_ret  = pct(price, prev_close)

    returns = {}
    for label, days in periods.items():
        target = end - timedelta(days=days)
        past   = df[df.index.date <= target]
        if not past.empty:
            returns[label] = pct(price, float(past["Close"].iloc[-1]))
        else:
            returns[label] = None

    return {
        "price":   price,
        "daily":   daily_ret,
        "returns": returns,
        "updated": last_date,
    }

def main():
    periods = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095}
    today   = datetime.utcnow().date().strftime("%Y-%m-%d")

    history  = load_json(HISTORY_FILE)
    snapshot = {}
    ok = 0

    for ticker in ETFS:
        print(f"Fetching {ticker}…")
        try:
            result = fetch_ticker_data(ticker, periods)
            if result is None:
                print(f"  ⚠ No data for {ticker}")
                continue

            snapshot[ticker] = result
            ok += 1

            # Persist price keyed by the actual last trading date
            if ticker not in history:
                history[ticker] = {}
            history[ticker][result["updated"]] = result["price"]

        except Exception as e:
            print(f"  ✗ {ticker}: {e}")

    snapshot["_benchmark"] = BENCHMARK
    snapshot["_updated"]   = today

    save_json(DATA_FILE, snapshot)

    # Trim history older than 4 years
    cutoff = (datetime.utcnow().date() - timedelta(days=4*365)).strftime("%Y-%m-%d")
    for tk in history:
        history[tk] = {d: v for d, v in history[tk].items() if d >= cutoff}
    save_json(HISTORY_FILE, history)

    print(f"\n✅ Done — {ok}/{len(ETFS)} ETFs saved.")

if __name__ == "__main__":
    main()
