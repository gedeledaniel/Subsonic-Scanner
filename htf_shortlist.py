from fastapi import FastAPI
import json, os

app = FastAPI(title="Subsonic 4H Scanner")

@app.get("/shortlist")
def shortlist():
    if not os.path.exists("scan_results.csv"):
        return {"error": "No scan results yet"}
    with open("scan_results.csv") as f:
        data = f.read()
    return {"message": "4H Scanner results", "data": data}
