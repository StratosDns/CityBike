from __future__ import annotations

from pathlib import Path

# Project root is assumed to be the parent of the src/ directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Raw storage locations for the ingestion layer.
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
TMP_DIR = DATA_DIR / "tmp"
LOG_DIR = PROJECT_ROOT / "logs"

# Citi Bike public sources.
CITIBIKE_SYSTEM_DATA_URL = "https://citibikenyc.com/system-data"
TRIPDATA_BUCKET_BASE_URL = "https://s3.amazonaws.com/tripdata"
GBFS_ROOT_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"

# Network defaults.
HTTP_TIMEOUT_SECONDS = 60
CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MB per chunk

# Common monthly trip-data filename patterns.
# Citi Bike has used more than one naming convention over time, so the loader
# checks several candidates for each requested month.
TRIPDATA_ARCHIVE_PATTERNS = (
    "{yyyymm}-citibike-tripdata.csv.zip",
    "{yyyymm}-citibike-tripdata.zip",
    "{yyyymm}-citibike-tripdata.csv.zip?download=1",
    "{yyyymm}-citibike-tripdata.zip?download=1",
)