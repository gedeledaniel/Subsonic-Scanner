#!/usr/bin/env python3
"""
HTF scanner (safe, with retries/timeouts + CSV output).
"""
import os
import sys
import time
import csv
import logging
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WATCHLIST = [
    "GBPUSD=X","EURUSD=X","EURGBP=X","GBPJPY=X","EURJPY=X",
    "XAUUSD=X","^GDAXI","^FTSE","USDCAD=X","USDJPY=X",
    "AUDUSD=X","NZDUSD=X","AUDJPY=X","NZDJPY=X","AUDCAD=X",
    "EURCAD=X","GBPCAD=X","XAGUSD=X","USDCHF=X","BZ=F"
]
NAME_MAP = {"^GDAXI":"DAX40","^FTSE":"FTSE100","BZ=F":"UKOIL"}
OUTPUT_CSV = "scan_results.csv"
HTF_INTERVAL = "4h"
PERIOD = "90d"
RETRIES = 3
BACKOFF_BASE = 2
TIMEOUT = 15

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def make_session():
    session = requests.Session()
    retries = Retry(total=RETRIES, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def download_with_retries(ticker, session, attempts=RETRIES):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            logging.info(f"Downloading {ticker} attempt {attempt}/{attempts}")
            df = yf.download(tickers=ticker, period=PERIOD, interval=HTF_INTERVAL, progress=False, threads=False, session=session)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
            logging.warning(f"No data returned for {ticker} on attempt {attempt}")
        except Exception as e:
            last_exc = e
            logging.warning(f"Error downloading {ticker} on attempt {attempt}: {e}")
        sleep = BACKOFF_BASE ** (attempt - 1)
        logging.info(f"Sleeping {sleep}s before retry")
        time.sleep(sleep)
    raise last_exc if last_exc else RuntimeError(f"Failed to download {ticker} after {attempts} attempts")

def compute_emas(df):
    close = df['Close'].dropna()
    ema34 = close.ewm(span=34, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()
    return ema34, ema200

def main():
    session = make_session()
    rows = []
    run_time = datetime.now(timezone.utc).isoformat()
    errors = []

    for ticker in WATCHLIST:
        friendly = NAME_MAP.get(ticker, ticker)
        try:
            df = download_with_retries(ticker, session, attempts=RETRIES)
            ema34, ema200 = compute_emas(df)
            last_close = df['Close'].dropna().iloc[-1]
            last_ema34 = float(ema34.iloc[-1]) if not ema34.empty else None
            last_ema200 = float(ema200.iloc[-1]) if not ema200.empty else None

            bias = "bull" if last_close > last_ema200 else ("bear" if last_close < last_ema200 else "neutral")
            momentum = "bull" if last_close > last_ema34 else ("bear" if last_close < last_ema34 else "neutral")

            cross = False
            try:
                diff = ema34 - ema200
                if len(diff.dropna()) >= 3:
                    last_vals = diff.dropna().iloc[-3:]
                    if (last_vals.iloc[0] * last_vals.iloc[-1]) < 0:
                        cross = True
            except Exception:
                cross = False

            score = 0
            if bias == momentum:
                score += 1

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
                "notes": ""
            })
            logging.info(f"{ticker}: close={last_close} ema34={last_ema34} ema200={last_ema200} bias={bias} momentum={momentum} cross={cross}")
        except Exception as e:
            logging.error(f"Failed to process {ticker}: {e}")
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

    fieldnames = ["run_time","ticker","symbol","last_close","ema34","ema200","bias","momentum","ema_cross_recent","score","notes"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    if len(errors) == len(WATCHLIST):
        logging.error("All downloads failed — exiting with error for visibility.")
        sys.exit(2)

    logging.info(f"Scan complete: wrote {len(rows)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
