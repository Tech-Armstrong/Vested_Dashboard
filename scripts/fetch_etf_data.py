import yfinance as yf
import json
import os
import sys
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
BENCHMARK    = "SPY"
DATA_FILE    = "docs/data/etf_data.json"
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

def extract_close(raw, ticker):
    """
    Handle yfinance MultiIndex columns which come as (Ticker, Field)
    when group_by='ticker', e.g. ('SPY', 'Close').
    """
    cols = raw.columns
    print(f"    DEBUG {ticker}: top-level keys = {list(cols.get_level_values(0).unique()[:5])}")

    # Try (Ticker, Field) order  — group_by='ticker'
    if (ticker, "Close") in cols:
        close = raw[(ticker, "Close")]
    # Try (Field, Ticker) order  — default yfinance
    elif ("Close", ticker) in cols:
        close = raw[("Close", ticker)]
    # Flat columns fallback (single ticker download)
    elif "Close" in cols:
        close = raw["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
    else:
        print(f"    DEBUG {ticker}: 'Close' not found in columns {list(cols[:8])}")
        return None

    close = pd.to_numeric(close, errors="coerce").dropna()
    return close if len(close) >= 2 else None

def main():
    periods = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095}
    today   = datetime.utcnow().date()
    today_s = today.strftime("%Y-%m-%d")
    history = load_json(HISTORY_FILE)

    end_s   = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    start_s = (today - timedelta(days=periods["3Y"] + 30)).strftime("%Y-%m-%d")

    print(f"Downloading {len(ETFS)} tickers  {start_s} → {end_s}")
    raw = yf.download(
        ETFS,
        start=start_s,
        end=end_s,
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )
    print(f"Raw shape: {raw.shape}  |  columns sample: {list(raw.columns[:6])}")

    snapshot = {}
    ok = 0

    for ticker in ETFS:
        try:
            close = extract_close(raw, ticker)
            if close is None:
                print(f"  ⚠ {ticker}: no usable data")
                continue

            price     = float(close.iloc[-1])
            last_date = close.index[-1].date().strftime("%Y-%m-%d")
            daily_ret = pct(price, float(close.iloc[-2]))

            rets = {}
            for label, days in periods.items():
                target = today - timedelta(days=days)
                past   = close[close.index.date <= target]
                rets[label] = pct(price, float(past.iloc[-1])) if not past.empty else None

            snapshot[ticker] = {
                "price":   round(price, 4),
                "daily":   daily_ret,
                "returns": rets,
                "updated": last_date,
            }
            history.setdefault(ticker, {})[last_date] = round(price, 4)
            ok += 1
            print(f"  ✓ {ticker}: ${price:.2f}  daily={daily_ret}%")

        except Exception as e:
            print(f"  ✗ {ticker}: {e}")

    if not ok:
        print("ERROR: 0 ETFs fetched — aborting.")
        sys.exit(1)

    snapshot["_benchmark"] = BENCHMARK
    snapshot["_updated"]   = today_s
    save_json(DATA_FILE, snapshot)

    cutoff = (today - timedelta(days=4 * 365)).strftime("%Y-%m-%d")
    for tk in history:
        history[tk] = {d: v for d, v in history[tk].items() if d >= cutoff}
    save_json(HISTORY_FILE, history)

    print(f"\n✅ Done — {ok}/{len(ETFS)} ETFs saved.")

if __name__ == "__main__":
    main()
