# Subsonic 4H Scanner (Live GitHub Auto)
Scans 20 forex and index pairs using 4H EMAs via yfinance.
Automatically updates every 4 hours via GitHub Actions.

### Setup
1. Create new GitHub repo.
2. Upload all files from this ZIP (root level).
3. Create PAT (classic) with 'repo' scope → copy token.
4. Go to Repo → Settings → Secrets → Actions → New secret:
   - Name: `PUSH_TOKEN`
   - Value: your PAT token.
5. Actions → Run workflow → select 'main' → Run.
6. View `scan_results.csv` in repo root or use `/shortlist` endpoint.
