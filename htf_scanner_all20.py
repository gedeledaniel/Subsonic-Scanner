#!/usr/bin/env python3
"""
HTF scanner (robust scalar extraction + retries).

Fixes:
- Avoid using float() on a pandas Series (FutureWarning).
- Avoid ambiguous truth-value comparisons by ensuring comparisons use scalars.
- Keep retries/backoff and let yfinance manage its own internal session.
"""
import os
import sys
import time
import csv
import logging
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf

# Configuration
WATCHLIST = [
    "GBPUSD=X","EURUSD=X","EURGBP=X","GBPJPY=X","EURJPY=X",
    "XAUUSD=X","^GDAXI","^FTSE","USDCAD=X","USDJPY=X",
    "AUDUSD=X","NZDUSD=X","AUDJPY=X","NZDJPY=X","AUDCAD=X",
    "EURCAD=X","GBPCAD=X","XAGUSD=X","USDCHF=X","BZ=F"
]
NAME_MAP = {
    "^GDAXI": "DAX40", "^FTSE": "FTSE100", "BZ=F": "UKOIL"
}

OUTPUT_CSV = "scan_results.csv"
HTF_INTERVAL = "4h"     # high timeframe used to compute EMAs
PERIOD = "90d"          # history length
RETRIES = 3
BACKOFF_BASE = 2        # seconds (exponential backoff base)

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def safe_last_scalar(series: pd.Series):
    """
    Return a Python float for the last non-NA element of a Series.
    If series is empty or has no valid values return None.
    """
    if series is None:
        return None
    s = series.dropna()
    if s.empty:
        return None
    try:
        # use iat for fastest scalar access
        val = s.iat[-1]
        # If it is a numpy scalar convert to Python float
        if hasattr(val, "item"):
            return float(val.item())
        return float(val)
    except Exception as e:
        logging.warning("safe_last_scalar: failed to extract scalar: %s", e)
        try:
            return float(s.iloc[-1])
        except Exception:
            return None


def download_with_retries(ticker, attempts=RETRIES):
    """
    Download historical data for `ticker` via yfinance.download().
    Do not pass a requests.Session — let yfinance control its transport.
    Retries on exception or empty DataFrame.
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            logging.info(f"Downloading {ticker} attempt {attempt}/{attempts} (no external session)")
            df = yf.download(tickers=ticker, period=PERIOD, interval=HTF_INTERVAL, progress=False, threads=False)
            if isinstance(df, pd.DataFrame) and not df.empty:
                logging.info(f"Downloaded {ticker} rows={len(df)}")
                return df
            else:
                logging.warning(f"No data returned for {ticker} on attempt {attempt}")
        except Exception as e:
            last_exc = e
            logging.warning(f"Error downloading {ticker} on attempt {attempt}: {e}")
        if attempt < attempts:
            sleep = BACKOFF_BASE ** (attempt - 1)
            logging.info(f"Sleeping {sleep}s before retry for {ticker}")
            time.sleep(sleep)
    raise last_exc if last_exc else RuntimeError(f"Failed to download {ticker} after {attempts} attempts")


def compute_emas(df):
    close = df['Close'].dropna()
    if close.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    ema34 = close.ewm(span=34, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()
    return ema34, ema200


def detect_recent_cross(ema34: pd.Series, ema200: pd.Series):
    """
    Returns True if a sign-change in (ema34 - ema200) occurred recently (last 3 valid points).
    Operates on Series; returns False if not enough data.
    """
    try:
        diff = (ema34 - ema200).dropna()
        if len(diff) < 3:
            return False
        last_vals = diff.iloc[-3:]
        # convert to scalars and check sign change from first to last
        a = float(last_vals.iat[0])
        b = float(last_vals.iat[-1])
        return (a * b) < 0
    except Exception:
        return False


def main():
    rows = []
    run_time = datetime.now(timezone.utc).isoformat()
    errors = []

    logging.info("Starting HTF scan (yfinance will manage sessions internally)")

    for ticker in WATCHLIST:
        friendly = NAME_MAP.get(ticker, ticker)
        try:
            df = download_with_retries(ticker, attempts=RETRIES)
            ema34, ema200 = compute_emas(df)

            # Extract scalars safely
            last_close = safe_last_scalar(df['Close']) if 'Close' in df else None
            last_ema34 = safe_last_scalar(ema34)
            last_ema200 = safe_last_scalar(ema200)

            if last_close is None:
                raise RuntimeError("No close price available for symbol")

            # Determine bias/momentum ensuring we're comparing scalars
            if last_ema200 is None:
                bias = "neutral"
            else:
                bias = "bull" if last_close > last_ema200 else ("bear" if last_close < last_ema200 else "neutral")

            if last_ema34 is None:
                momentum = "neutral"
            else:
                momentum = "bull" if last_close > last_ema34 else ("bear" if last_close < last_ema34 else "neutral")

            cross = detect_recent_cross(ema34, ema200)

            score = 0
            if bias == momentum and bias != "neutral":
                score += 1

            notes = ""
            rows.append({
                "run_time": run_time,
                "ticker": friendly,
                "symbol": ticker,
                "last_close": last_close,
                "ema34": last_ema34,
                "ema200": last_ema200,
                "bias": bias,
                "momentum": momentum,
                "ema_cross_recent": cross,
                "score": score,
                "notes": notes
            })

            logging.info("%s: close=%s ema34=%s ema200=%s bias=%s momentum=%s cross=%s",
                         ticker, last_close, last_ema34, last_ema200, bias, momentum, cross)

        except Exception as e:
            logging.error("Failed to process %s: %s", ticker, e)
            errors.append({"ticker": ticker, "error": str(e)})
            rows.append({
                "run_time": run_time,
                "ticker": NAME_MAP.get(ticker, ticker),
                "symbol": ticker,
                "last_close": "",
                "ema34": "",
                "ema200": "",
                "bias": "error",
                "momentum": "error",
                "ema_cross_recent": "",
                "score": "",
                "notes": f"download error: {e}"
            })

    # Write CSV (overwrite each run)
    fieldnames = ["run_time","ticker","symbol","last_close","ema34","ema200","bias","momentum","ema_cross_recent","score","notes"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    # Exit non-zero if all symbols errored (so Action shows failure)
    if len(errors) == len(WATCHLIST):
        logging.error("All downloads failed — exiting with error for visibility.")
        sys.exit(2)

    logging.info(f"Scan complete: wrote {len(rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main() 
