# Problem 

Understand urban bike demand and operational bottlenecks

# Data

Official Citi Bike trip history..

to be enrichened with NOAA weather data

# Pipeline 

Automated monthly ingestion, cleaning, transformation and dashboard refresh

# Outputs 

KPIs, trend charts, station analysis, segment analysis

# Business value

improves operational rebalancing, demand understanding and growth targeting


# Citi Bike Ingestion Commands

## Create environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Download a fixed month range

```bash
python src/load_tripdata.py --start-month 2024-01 --end-month 2024-03
```

## Download specific months

```bash
python src/load_tripdata.py --months 2024-01 2024-06 2024-12
```

## Force redownload

```bash
python src/load_tripdata.py --start-month 2024-01 --end-month 2024-03 --overwrite
```

## Download live GBFS snapshot

```bash
python src/load_gbfs_snapshot.py --feeds station_information station_status
```


### Expected bronze output structure
```text
data/
  bronze/
    tripdata/
      202401/
        archive/
          202401-citibike-tripdata.csv.zip
        extracted/
          202401-citibike-tripdata_1.csv
          202401-citibike-tripdata_2.csv
        manifest.json
    gbfs/
      en/
        station_information.json
        station_status.json
```