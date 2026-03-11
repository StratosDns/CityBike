# Local Pipeline Commands

## 1. Download bronze data
```bash
python src/load_tripdata.py --start-month 2024-01 --end-month 2024-03
python src/load_gbfs_snapshot.py --feeds station_information station_status
```

## 2. Build silver parquet

```bash
python src/preprocess_tripdata.py --months 2024-01 2024-02 2024-03
```

## 3. Build gold analytics outputs

```bash
python src/build_marts.py --months 2024-01 2024-02 2024-03 --publish
```

## Output layers

- data/bronze/: raw downloaded archives, extracted CSVs, GBFS snapshots

- data/silver/: cleaned trip-level parquet files

- data/gold/: analytics tables for KPIs and charts

- data/published/: dashboard-ready published CSV files

