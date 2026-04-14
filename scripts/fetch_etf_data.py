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

def get_close_series(raw, ticker, n_tickers):
    """
    Safely extract a plain 1-D Close price Series regardless of
    whether yfinance returned a flat or MultiIndex column structure.
    """
    if n_tickers == 1:
        # Single ticker: columns may be flat ["Close"] or MultiIndex [("Close","SPY")]
        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"].squeeze()
        else:
            close = raw["Close"]
    else:
        # Multiple tickers: columns are MultiIndex [("Close","SPY"), ...]
        if ("Close", ticker) in raw.columns:
            close = raw["Close"][ticker]
        elif "Close" in raw.columns:
            # Flat fallback (shouldn't happen with group_by="ticker" but just in case)
            close = raw["Close"]
        else:
            return None

    # Ensure it's a plain Series of floats, drop NaNs
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]   # take first column if still 2-D
    close = pd.to_numeric(close, errors="coerce").dropna()
    return close if not close.empty else None

def main():
    periods  = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095}
    today    = datetime.utcnow().date()
    today_s  = today.strftime("%Y-%m-%d")
    history  = load_json(HISTORY_FILE)

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
    print(f"Raw shape: {raw.shape}  |  column type: {type(raw.columns).__name__}")

    snapshot = {}
    ok = 0

    for ticker in ETFS:
        try:
            close = get_close_series(raw, ticker, len(ETFS))
            if close is None or len(close) < 2:
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

            # Persist to history
            history.setdefault(ticker, {})[last_date] = round(price, 4)
            ok += 1
            print(f"  ✓ {ticker}: ${price:.2f}  daily={daily_ret}%")

        except Exception as e:
            print(f"  ✗ {ticker}: {e}")

    if not ok:
        print("ERROR: 0 ETFs fetched — aborting to preserve existing data.")
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
