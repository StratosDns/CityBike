# Bronze Refresh Workflow

## Purpose
This workflow validates public data ingestion on a schedule and stores the resulting manifests, GBFS snapshots, and logs as workflow artifacts.

## Triggers
- Manual run from the Actions tab with custom month inputs
- Weekly scheduled run on Sunday at 03:17 UTC

## What it does
1. Checks out the repository
2. Installs Python dependencies
3. Downloads Citi Bike monthly trip archives for the requested month range
4. Downloads GBFS snapshots for `station_information` and `station_status`
5. Uploads logs, manifests, and GBFS JSON files as workflow artifacts

## What it does not do yet
- Preprocess bronze data into silver parquet
- Build gold analytics tables
- Commit published outputs back to the repository
- Refresh a public dashboard

## Manual run inputs
- `start_month`: inclusive start month in `YYYY-MM`
- `end_month`: inclusive end month in `YYYY-MM`
- `fetch_gbfs`: whether to download GBFS station snapshots

## Expected artifacts
- `artifacts/run_metadata.txt`
- `logs/*.log`
- `data/bronze/**/manifest.json`
- `data/bronze/gbfs/**/*.json`