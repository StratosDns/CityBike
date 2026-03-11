# Dashboard Commands

## Start the dashboard locally
```bash
streamlit run dashboard/app.py
```

## Rebuild data before opening the dashboard

```bash
python src/preprocess_tripdata.py --start-month 2024-01 --end-month 2024-03
python src/build_marts.py --start-month 2024-01 --end-month 2024-03 --publish
streamlit run dashboard/app.py
```


Run this locally:

```bash
streamlit run dashboard/app.py
```

