import yfinance as yf
import pandas as pd
from datetime import datetime

GROUP_A = ["GBPUSD=X", "EURUSD=X", "EURGBP=X", "GBPJPY=X", "EURJPY=X", "XAUUSD=X", "^GDAXI", "^FTSE", "CAD=X", "JPY=X"]
GROUP_B = ["AUDUSD=X", "NZDUSD=X", "AUDJPY=X", "NZDJPY=X", "AUDCAD=X", "EURCAD=X", "GBPCAD=X", "XAGUSD=X", "USDCHF=X", "BZ=F"]

def get_bias(df):
    df["EMA200"] = df["Close"].ewm(span=200).mean()
    df["EMA34"] = df["Close"].ewm(span=34).mean()
    return df

def check_alignment(df):
    return df["EMA34"].iloc[-1] > df["EMA200"].iloc[-1]

def scan_pairs():
    results = {"timestamp": datetime.utcnow().isoformat(), "pairs": {}}
    for symbol in GROUP_A + GROUP_B:
        df = yf.download(symbol, period="30d", interval="4h", progress=False)
        if df.empty:
            continue
        df = get_bias(df)
        aligned = check_alignment(df)
        group = "A" if symbol in GROUP_A else "B"
        results["pairs"][symbol] = {"group": group, "bias_up": aligned}
    pd.DataFrame.from_dict(results["pairs"], orient="index").to_csv("scan_results.csv")
    print("Scan completed:", datetime.utcnow())
    return results

if __name__ == "__main__":
    scan_pairs()
