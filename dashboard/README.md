# Dashboard

## Local run
```bash
streamlit run dashboard/app.py
```

## Data source

- The dashboard reads only from:

- data/published/daily_kpis.csv

- data/published/hourly_demand.csv

- data/published/member_vs_casual.csv

- data/published/top_start_stations.csv

- data/published/top_end_stations.csv

- data/published/station_imbalance.csv

- data/published/duration_summary.csv

## Expected pipeline

1. Bronze ingestion creates raw source files

2. Preprocessing creates silver parquet files

3. Gold marts create analytics tables

4. Published outputs are copied into data/published/

5. Streamlit reads those published files


### `.streamlit/config.toml`
```toml
[server]
headless = true

[theme]
base = "light"
```