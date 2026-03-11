from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

from config import DATA_DIR
from preprocessing import GOLD_DIR, load_silver_trips
from utils import ensure_directory

LOGGER = logging.getLogger(__name__)

PUBLISHED_DIR = DATA_DIR / "published"


def _save_csv(df: pd.DataFrame, output_path: Path) -> Path:
    """
    Save a dataframe as CSV with stable formatting.
    """
    ensure_directory(output_path.parent)
    df.to_csv(output_path, index=False)
    return output_path


def build_daily_kpis(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Daily ridership and duration summary.
    """
    daily = (
        trips.groupby("ride_date", dropna=False)
        .agg(
            total_rides=("ride_id", "count"),
            unique_start_stations=("start_station_id", "nunique"),
            unique_end_stations=("end_station_id", "nunique"),
            avg_ride_duration_minutes=("ride_duration_minutes", "mean"),
            median_ride_duration_minutes=("ride_duration_minutes", "median"),
            member_rides=("member_type", lambda s: int((s == "member").sum())),
            casual_rides=("member_type", lambda s: int((s == "casual").sum())),
        )
        .reset_index()
        .sort_values("ride_date")
    )

    daily["member_share"] = daily["member_rides"] / daily["total_rides"]
    daily["casual_share"] = daily["casual_rides"] / daily["total_rides"]

    return daily


def build_hourly_demand(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Hourly demand by weekday and rider segment.
    """
    hourly = (
        trips.groupby(["day_name", "day_of_week", "start_hour", "member_type"], dropna=False)
        .agg(
            total_rides=("ride_id", "count"),
            avg_ride_duration_minutes=("ride_duration_minutes", "mean"),
        )
        .reset_index()
        .sort_values(["day_of_week", "start_hour", "member_type"])
    )

    return hourly


def build_member_vs_casual(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Monthly rider mix summary.
    """
    rider_mix = (
        trips.groupby(["year_month", "member_type"], dropna=False)
        .agg(
            total_rides=("ride_id", "count"),
            avg_ride_duration_minutes=("ride_duration_minutes", "mean"),
        )
        .reset_index()
        .sort_values(["year_month", "member_type"])
    )

    totals = rider_mix.groupby("year_month")["total_rides"].transform("sum")
    rider_mix["ride_share"] = rider_mix["total_rides"] / totals

    return rider_mix


def build_top_start_stations(trips: pd.DataFrame, top_n: int = 50) -> pd.DataFrame:
    """
    Top origin stations ranked by trip starts.
    """
    starts = (
        trips.groupby(["start_station_id", "start_station_name"], dropna=False)
        .agg(
            total_starts=("ride_id", "count"),
            avg_ride_duration_minutes=("ride_duration_minutes", "mean"),
        )
        .reset_index()
        .sort_values("total_starts", ascending=False)
        .head(top_n)
    )

    return starts


def build_top_end_stations(trips: pd.DataFrame, top_n: int = 50) -> pd.DataFrame:
    """
    Top destination stations ranked by trip ends.
    """
    ends = (
        trips.groupby(["end_station_id", "end_station_name"], dropna=False)
        .agg(
            total_ends=("ride_id", "count"),
            avg_ride_duration_minutes=("ride_duration_minutes", "mean"),
        )
        .reset_index()
        .sort_values("total_ends", ascending=False)
        .head(top_n)
    )

    return ends


def build_station_imbalance(trips: pd.DataFrame, min_volume: int = 100) -> pd.DataFrame:
    """
    Station imbalance defined as total starts minus total ends.
    """
    starts = (
        trips.groupby(["start_station_id", "start_station_name"], dropna=False)
        .agg(total_starts=("ride_id", "count"))
        .reset_index()
        .rename(
            columns={
                "start_station_id": "station_id",
                "start_station_name": "station_name",
            }
        )
    )

    ends = (
        trips.groupby(["end_station_id", "end_station_name"], dropna=False)
        .agg(total_ends=("ride_id", "count"))
        .reset_index()
        .rename(
            columns={
                "end_station_id": "station_id",
                "end_station_name": "station_name",
            }
        )
    )

    imbalance = starts.merge(
        ends,
        on=["station_id", "station_name"],
        how="outer",
    ).fillna({"total_starts": 0, "total_ends": 0})

    imbalance["total_starts"] = imbalance["total_starts"].astype(int)
    imbalance["total_ends"] = imbalance["total_ends"].astype(int)
    imbalance["net_imbalance"] = imbalance["total_starts"] - imbalance["total_ends"]
    imbalance["total_volume"] = imbalance["total_starts"] + imbalance["total_ends"]

    imbalance = imbalance[imbalance["total_volume"] >= min_volume].sort_values(
        "net_imbalance",
        ascending=False,
    )

    return imbalance


def build_duration_summary(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Duration summary by rider segment.
    """
    summary = (
        trips.groupby("member_type", dropna=False)
        .agg(
            total_rides=("ride_id", "count"),
            avg_ride_duration_minutes=("ride_duration_minutes", "mean"),
            median_ride_duration_minutes=("ride_duration_minutes", "median"),
            p90_ride_duration_minutes=("ride_duration_minutes", lambda s: s.quantile(0.9)),
        )
        .reset_index()
        .sort_values("total_rides", ascending=False)
    )

    return summary


def build_all_marts(months: Iterable[str] | None = None) -> dict[str, Path]:
    """
    Build all gold analytics tables from silver trips data.
    """
    trips = load_silver_trips(months=months)

    output_paths = {
        "daily_kpis": _save_csv(build_daily_kpis(trips), GOLD_DIR / "daily_kpis.csv"),
        "hourly_demand": _save_csv(build_hourly_demand(trips), GOLD_DIR / "hourly_demand.csv"),
        "member_vs_casual": _save_csv(build_member_vs_casual(trips), GOLD_DIR / "member_vs_casual.csv"),
        "top_start_stations": _save_csv(build_top_start_stations(trips), GOLD_DIR / "top_start_stations.csv"),
        "top_end_stations": _save_csv(build_top_end_stations(trips), GOLD_DIR / "top_end_stations.csv"),
        "station_imbalance": _save_csv(build_station_imbalance(trips), GOLD_DIR / "station_imbalance.csv"),
        "duration_summary": _save_csv(build_duration_summary(trips), GOLD_DIR / "duration_summary.csv"),
    }

    return output_paths


def publish_gold_outputs() -> list[Path]:
    """
    Copy gold CSV outputs into a dashboard-friendly published directory.
    """
    ensure_directory(PUBLISHED_DIR)
    published_paths: list[Path] = []

    for csv_path in sorted(GOLD_DIR.glob("*.csv")):
        destination = PUBLISHED_DIR / csv_path.name
        destination.write_bytes(csv_path.read_bytes())
        published_paths.append(destination)

    LOGGER.info("Published %s gold output file(s).", len(published_paths))
    return published_paths