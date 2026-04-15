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

# Trading day periods (industry standard — same as Bloomberg/Morningstar)
# ~21 trading days per month
PERIODS = {"1M": 21, "3M": 63, "6M": 126, "1Y": 252, "3Y": 756}

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
    Safely extract a 1-D Close price Series from yfinance's MultiIndex output.
    Handles both (Ticker, Field) and (Field, Ticker) column orderings.
    """
    cols = raw.columns

    if (ticker, "Close") in cols:
        close = raw[(ticker, "Close")]
    elif ("Close", ticker) in cols:
        close = raw[("Close", ticker)]
    elif "Close" in cols:
        close = raw["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
    else:
        print(f"    ⚠ {ticker}: 'Close' not found. Cols sample: {list(cols[:6])}")
        return None

    close = pd.to_numeric(close, errors="coerce").dropna()
    return close if len(close) >= 2 else None

def main():
    today   = datetime.utcnow().date()
    today_s = today.strftime("%Y-%m-%d")
    history = load_json(HISTORY_FILE)

    end_s   = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    # Need ~1150 calendar days to guarantee 756 trading days for 3Y
    start_s = (today - timedelta(days=1150)).strftime("%Y-%m-%d")

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
    print(f"Raw shape: {raw.shape}  |  columns type: {type(raw.columns).__name__}")

    snapshot = {}
    ok = 0

    for ticker in ETFS:
        try:
            close = extract_close(raw, ticker)
            if close is None or len(close) < 2:
                print(f"  ⚠ {ticker}: no usable data")
                continue

            price     = float(close.iloc[-1])
            last_date = close.index[-1].date().strftime("%Y-%m-%d")
            daily_ret = pct(price, float(close.iloc[-2]))

            # Calculate returns using trading day row offsets (not calendar days)
            rets = {}
            for label, tdays in PERIODS.items():
                if len(close) > tdays:
                    past_price = float(close.iloc[-(tdays + 1)])
                    rets[label] = pct(price, past_price)
                else:
                    rets[label] = None  # Not enough history yet

            snapshot[ticker] = {
                "price":   round(price, 4),
                "daily":   daily_ret,
                "returns": rets,
                "updated": last_date,
            }

            # Save daily close to persistent history
            history.setdefault(ticker, {})[last_date] = round(price, 4)
            ok += 1
            print(f"  ✓ {ticker}: ${price:.2f}  daily={daily_ret}%  1Y={rets.get('1Y')}%")

        except Exception as e:
            print(f"  ✗ {ticker}: {e}")

    if not ok:
        print("ERROR: 0 ETFs fetched — aborting to preserve existing data.")
        sys.exit(1)

    snapshot["_benchmark"] = BENCHMARK
    snapshot["_updated"]   = today_s
    save_json(DATA_FILE, snapshot)

    # Trim history older than 4 years
    cutoff = (today - timedelta(days=4 * 365)).strftime("%Y-%m-%d")
    for tk in history:
        history[tk] = {d: v for d, v in history[tk].items() if d >= cutoff}
    save_json(HISTORY_FILE, history)

    print(f"\n✅ Done — {ok}/{len(ETFS)} ETFs saved.")

if __name__ == "__main__":
    main()
