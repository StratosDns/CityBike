from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from config import BRONZE_DIR, DATA_DIR
from utils import ensure_directory, write_json

LOGGER = logging.getLogger(__name__)

SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"


CANONICAL_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_type",
    "ride_duration_minutes",
    "ride_date",
    "start_hour",
    "day_of_week",
    "day_name",
    "is_weekend",
    "year_month",
]


EXPECTED_SOURCE_COLUMNS = {
    "ride_id": "ride_id",
    "rideable_type": "rideable_type",
    "started_at": "started_at",
    "ended_at": "ended_at",
    "start_station_id": "start_station_id",
    "start_station_name": "start_station_name",
    "end_station_id": "end_station_id",
    "end_station_name": "end_station_name",
    "start_lat": "start_lat",
    "start_lng": "start_lng",
    "end_lat": "end_lat",
    "end_lng": "end_lng",
    "member_casual": "member_type",
}


@dataclass(frozen=True)
class PreprocessSummary:
    month: str
    files_read: int
    rows_raw: int
    rows_after_basic_cleaning: int
    rows_after_duration_filter: int
    rows_final: int
    duplicates_removed: int
    missing_timestamps_removed: int
    invalid_duration_removed: int
    output_path: str


def list_extracted_csv_files(month: str) -> list[Path]:
    """
    Return all extracted CSV files for a given YYYY-MM month string.
    """
    normalized = month.replace("-", "")
    extracted_dir = BRONZE_DIR / "tripdata" / normalized / "extracted"

    if not extracted_dir.exists():
        raise FileNotFoundError(
            f"No extracted bronze directory found for month {month}: {extracted_dir}"
        )

    csv_files = sorted(extracted_dir.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {extracted_dir}")

    return csv_files


def discover_available_months() -> list[str]:
    """
    Discover all available months already downloaded in the bronze layer.
    """
    tripdata_root = BRONZE_DIR / "tripdata"
    if not tripdata_root.exists():
        return []

    months: list[str] = []
    for child in sorted(tripdata_root.iterdir()):
        if child.is_dir() and len(child.name) == 6 and child.name.isdigit():
            months.append(f"{child.name[:4]}-{child.name[4:6]}")
    return months


def read_month_csvs(month: str) -> pd.DataFrame:
    """
    Read and concatenate all CSV files for the requested month.
    """
    csv_files = list_extracted_csv_files(month)
    frames: list[pd.DataFrame] = []

    for csv_file in csv_files:
        LOGGER.info("Reading %s", csv_file)
        frame = pd.read_csv(csv_file, low_memory=False)
        frame["source_file"] = csv_file.name
        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    return combined


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename known Citi Bike columns to a canonical raw schema and create any missing raw columns.
    """
    output = df.rename(columns=EXPECTED_SOURCE_COLUMNS).copy()

    required_raw_columns = [
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_id",
        "start_station_name",
        "end_station_id",
        "end_station_name",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_type",
    ]

    for column in required_raw_columns:
        if column not in output.columns:
            output[column] = pd.NA

    ordered_existing_columns = list(dict.fromkeys(required_raw_columns + list(output.columns)))
    output = output[ordered_existing_columns]

    return output


def normalize_string_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """
    Strip whitespace and normalize empty strings to missing values.
    """
    output = df.copy()

    for column in columns:
        if column not in output.columns:
            continue

        output[column] = output[column].astype("string").str.strip()
        output[column] = output[column].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    return output


def cast_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert timestamp columns to pandas datetime.
    """
    output = df.copy()
    output["started_at"] = pd.to_datetime(output["started_at"], errors="coerce", utc=False)
    output["ended_at"] = pd.to_datetime(output["ended_at"], errors="coerce", utc=False)
    return output


def cast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert coordinate columns to numeric types.
    """
    output = df.copy()

    numeric_columns = ["start_lat", "start_lng", "end_lat", "end_lng"]
    for column in numeric_columns:
        output[column] = pd.to_numeric(output[column], errors="coerce")

    return output


def derive_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived analytical columns used downstream in marts.
    """
    output = df.copy()

    output["ride_duration_minutes"] = (
        (output["ended_at"] - output["started_at"]).dt.total_seconds() / 60.0
    )

    output["ride_date"] = output["started_at"].dt.date
    output["start_hour"] = output["started_at"].dt.hour
    output["day_of_week"] = output["started_at"].dt.dayofweek
    output["day_name"] = output["started_at"].dt.day_name()
    output["is_weekend"] = output["day_of_week"].isin([5, 6])
    output["year_month"] = output["started_at"].dt.strftime("%Y-%m")

    output["member_type"] = (
        output["member_type"]
        .astype("string")
        .str.lower()
        .replace(
            {
                "subscriber": "member",
                "customer": "casual",
            }
        )
    )

    return output


def filter_invalid_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Apply conservative cleaning rules for timestamps, duplicates, and ride duration.
    """
    output = df.copy()

    missing_timestamps_removed = int(
        output["started_at"].isna().sum() + output["ended_at"].isna().sum()
    )

    output = output.dropna(subset=["started_at", "ended_at"])

    pre_dedup_count = len(output)
    output = output.drop_duplicates(subset=["ride_id"], keep="first")
    duplicates_removed = pre_dedup_count - len(output)

    output = derive_features(output)

    pre_duration_count = len(output)
    output = output[
        output["ride_duration_minutes"].notna()
        & (output["ride_duration_minutes"] >= 1)
        & (output["ride_duration_minutes"] <= 24 * 60)
    ].copy()
    invalid_duration_removed = pre_duration_count - len(output)

    metrics = {
        "duplicates_removed": duplicates_removed,
        "missing_timestamps_removed": missing_timestamps_removed,
        "invalid_duration_removed": invalid_duration_removed,
    }

    return output, metrics


def reorder_canonical_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder output columns so the most important analytical fields appear first.
    """
    front_columns = [column for column in CANONICAL_COLUMNS if column in df.columns]
    remaining_columns = [column for column in df.columns if column not in front_columns]
    return df[front_columns + remaining_columns]


def preprocess_month(month: str) -> PreprocessSummary:
    """
    Build one silver parquet dataset for a specific month.
    """
    normalized_month = month.replace("-", "")
    output_dir = SILVER_DIR / "trips" / f"year_month={month}"
    ensure_directory(output_dir)
    output_path = output_dir / "trips.parquet"

    raw_df = read_month_csvs(month)
    rows_raw = len(raw_df)
    files_read = len(list_extracted_csv_files(month))

    standardized = standardize_columns(raw_df)
    standardized = normalize_string_columns(
        standardized,
        columns=[
            "ride_id",
            "rideable_type",
            "start_station_id",
            "start_station_name",
            "end_station_id",
            "end_station_name",
            "member_type",
        ],
    )
    standardized = cast_timestamps(standardized)
    standardized = cast_numeric_columns(standardized)

    rows_after_basic_cleaning = len(standardized)

    cleaned, metrics = filter_invalid_rows(standardized)
    rows_after_duration_filter = len(cleaned)

    cleaned = reorder_canonical_columns(cleaned)
    cleaned.to_parquet(output_path, index=False)

    summary = PreprocessSummary(
        month=month,
        files_read=files_read,
        rows_raw=rows_raw,
        rows_after_basic_cleaning=rows_after_basic_cleaning,
        rows_after_duration_filter=rows_after_duration_filter,
        rows_final=len(cleaned),
        duplicates_removed=metrics["duplicates_removed"],
        missing_timestamps_removed=metrics["missing_timestamps_removed"],
        invalid_duration_removed=metrics["invalid_duration_removed"],
        output_path=str(output_path),
    )

    summary_path = output_dir / "summary.json"
    write_json(summary.__dict__, summary_path)

    LOGGER.info(
        "Preprocessed %s: raw=%s final=%s output=%s",
        month,
        rows_raw,
        len(cleaned),
        output_path,
    )

    return summary


def preprocess_months(months: Iterable[str]) -> list[PreprocessSummary]:
    """
    Build silver datasets for multiple months.
    """
    summaries: list[PreprocessSummary] = []

    for month in months:
        summaries.append(preprocess_month(month))

    return summaries


def load_silver_trips(months: Iterable[str] | None = None) -> pd.DataFrame:
    """
    Load one or more silver parquet partitions into a single dataframe.
    """
    if months is None:
        months = discover_available_months()

    parquet_paths: list[Path] = []
    for month in months:
        parquet_path = SILVER_DIR / "trips" / f"year_month={month}" / "trips.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(f"Silver parquet not found for month {month}: {parquet_path}")
        parquet_paths.append(parquet_path)

    frames = [pd.read_parquet(path) for path in parquet_paths]
    if not frames:
        raise FileNotFoundError("No silver parquet files were found to load.")

    return pd.concat(frames, ignore_index=True, sort=False)