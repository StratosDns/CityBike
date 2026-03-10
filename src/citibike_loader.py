from __future__ import annotations

import calendar
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import requests
from dateutil.relativedelta import relativedelta

from config import (
    BRONZE_DIR,
    CHUNK_SIZE_BYTES,
    GBFS_ROOT_URL,
    HTTP_TIMEOUT_SECONDS,
    TMP_DIR,
    TRIPDATA_ARCHIVE_PATTERNS,
    TRIPDATA_BUCKET_BASE_URL,
)
from utils import ensure_directory, write_json

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MonthSpec:
    """
    Normalized year-month container for building Citi Bike archive URLs.
    """

    year: int
    month: int

    @property
    def yyyymm(self) -> str:
        return f"{self.year:04d}{self.month:02d}"

    @property
    def first_day(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def month_name(self) -> str:
        return calendar.month_name[self.month]


@dataclass(frozen=True)
class ArchiveResolution:
    """
    Information about the archive that was resolved for a month.
    """

    month: MonthSpec
    url: str
    filename: str


def parse_month(value: str) -> MonthSpec:
    """
    Parse a YYYY-MM string into a MonthSpec.
    """
    match = re.fullmatch(r"(\d{4})-(\d{2})", value)
    if match is None:
        raise ValueError(f"Invalid month '{value}'. Expected format: YYYY-MM")

    year = int(match.group(1))
    month = int(match.group(2))

    if month < 1 or month > 12:
        raise ValueError(f"Invalid month '{value}'. Month must be between 01 and 12")

    return MonthSpec(year=year, month=month)


def month_range(start_month: str, end_month: str) -> list[MonthSpec]:
    """
    Generate all months in the inclusive range [start_month, end_month].
    """
    start = parse_month(start_month)
    end = parse_month(end_month)

    if start.first_day > end.first_day:
        raise ValueError("start_month must be less than or equal to end_month")

    current = start.first_day
    output: list[MonthSpec] = []

    while current <= end.first_day:
        output.append(MonthSpec(year=current.year, month=current.month))
        current += relativedelta(months=1)

    return output


def create_session() -> requests.Session:
    """
    Create an HTTP session with a descriptive user-agent for public data access.
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "urban-mobility-monitor/1.0 "
                "(public Citi Bike ingestion pipeline; contact via repository)"
            )
        }
    )
    return session


def build_tripdata_candidates(month: MonthSpec) -> list[str]:
    """
    Build candidate archive URLs for a given month.

    Citi Bike's public trip-history bucket has used multiple archive naming
    conventions over time, so the loader checks several candidates.
    """
    candidates: list[str] = []

    for pattern in TRIPDATA_ARCHIVE_PATTERNS:
        filename = pattern.format(yyyymm=month.yyyymm)
        candidates.append(f"{TRIPDATA_BUCKET_BASE_URL}/{filename}")

    return candidates


def resolve_tripdata_archive(
    session: requests.Session,
    month: MonthSpec,
) -> ArchiveResolution:
    """
    Resolve the first available trip archive URL for the requested month.

    Raises:
        FileNotFoundError: if no candidate URL resolves successfully.
    """
    candidates = build_tripdata_candidates(month)

    for url in candidates:
        try:
            response = session.head(url, allow_redirects=True, timeout=HTTP_TIMEOUT_SECONDS)
            if response.ok:
                filename = url.split("/")[-1].split("?")[0]
                return ArchiveResolution(month=month, url=url, filename=filename)
        except requests.RequestException:
            continue

    raise FileNotFoundError(
        f"Could not resolve a trip archive for {month.yyyymm}. "
        f"Checked {len(candidates)} candidate URLs."
    )


def download_file(
    session: requests.Session,
    url: str,
    destination: Path,
    overwrite: bool = False,
) -> Path:
    """
    Download a file via streaming to avoid loading the entire response in memory.
    """
    ensure_directory(destination.parent)

    if destination.exists() and not overwrite:
        LOGGER.info("Using existing file: %s", destination)
        return destination

    LOGGER.info("Downloading %s -> %s", url, destination)

    with session.get(url, stream=True, timeout=HTTP_TIMEOUT_SECONDS) as response:
        response.raise_for_status()

        with destination.open("wb") as output_file:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                if chunk:
                    output_file.write(chunk)

    return destination


def extract_zip_archive(zip_path: Path, output_dir: Path, overwrite: bool = False) -> list[Path]:
    """
    Extract all files from a ZIP archive.

    Citi Bike may include multiple CSV files in a single monthly archive for
    high-volume months, so this function extracts every member.
    """
    ensure_directory(output_dir)
    extracted_paths: list[Path] = []

    LOGGER.info("Extracting archive: %s", zip_path)

    with zipfile.ZipFile(zip_path, mode="r") as zip_file:
        for member in zip_file.infolist():
            member_path = output_dir / member.filename

            if member.is_dir():
                ensure_directory(member_path)
                continue

            ensure_directory(member_path.parent)

            if member_path.exists() and not overwrite:
                LOGGER.info("Using existing extracted file: %s", member_path)
                extracted_paths.append(member_path)
                continue

            with zip_file.open(member) as source, member_path.open("wb") as target:
                target.write(source.read())

            extracted_paths.append(member_path)

    return extracted_paths


def ingest_trip_month(
    session: requests.Session,
    month: MonthSpec,
    overwrite: bool = False,
) -> dict:
    """
    Download and extract a single month of Citi Bike trip data into the bronze layer.

    Output structure:
        data/bronze/tripdata/<YYYYMM>/archive.zip
        data/bronze/tripdata/<YYYYMM>/extracted/*.csv
        data/bronze/tripdata/<YYYYMM>/manifest.json
    """
    month_root = BRONZE_DIR / "tripdata" / month.yyyymm
    archive_dir = month_root / "archive"
    extracted_dir = month_root / "extracted"
    manifest_path = month_root / "manifest.json"

    resolution = resolve_tripdata_archive(session=session, month=month)
    archive_path = archive_dir / resolution.filename

    download_file(
        session=session,
        url=resolution.url,
        destination=archive_path,
        overwrite=overwrite,
    )

    extracted_paths = extract_zip_archive(
        zip_path=archive_path,
        output_dir=extracted_dir,
        overwrite=overwrite,
    )

    manifest = {
        "month": month.yyyymm,
        "source_url": resolution.url,
        "archive_path": str(archive_path),
        "extracted_files": [str(path) for path in sorted(extracted_paths)],
        "record_count": None,
        "status": "downloaded_and_extracted",
    }
    write_json(manifest, manifest_path)

    LOGGER.info(
        "Completed ingest for %s. Extracted %s files.",
        month.yyyymm,
        len(extracted_paths),
    )

    return manifest


def ingest_trip_range(
    start_month: str,
    end_month: str,
    overwrite: bool = False,
) -> list[dict]:
    """
    Download and extract all monthly archives in the inclusive date range.
    """
    months = month_range(start_month=start_month, end_month=end_month)
    session = create_session()

    manifests: list[dict] = []

    for month in months:
        manifests.append(
            ingest_trip_month(
                session=session,
                month=month,
                overwrite=overwrite,
            )
        )

    return manifests


def fetch_gbfs_root(session: requests.Session | None = None) -> dict:
    """
    Fetch the official Citi Bike GBFS root feed.
    """
    owns_session = session is None
    session = session or create_session()

    try:
        response = session.get(GBFS_ROOT_URL, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    finally:
        if owns_session:
            session.close()


def snapshot_gbfs_feed(
    feed_name: str,
    language: str = "en",
    output_dir: Path | None = None,
) -> Path:
    """
    Download one GBFS feed and save it as a JSON snapshot in the bronze layer.

    Example feed names:
        - station_information
        - station_status
        - system_information
    """
    session = create_session()
    gbfs_root = fetch_gbfs_root(session=session)

    feeds = gbfs_root.get("data", {}).get(language, {}).get("feeds", [])
    feed_map = {item["name"]: item["url"] for item in feeds if "name" in item and "url" in item}

    if feed_name not in feed_map:
        available = ", ".join(sorted(feed_map.keys()))
        raise KeyError(
            f"Feed '{feed_name}' not found for language '{language}'. "
            f"Available feeds: {available}"
        )

    destination_dir = output_dir or (BRONZE_DIR / "gbfs" / language)
    ensure_directory(destination_dir)

    destination = destination_dir / f"{feed_name}.json"
    LOGGER.info("Downloading GBFS feed '%s' -> %s", feed_name, destination)

    response = session.get(feed_map[feed_name], timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()

    destination.write_text(response.text, encoding="utf-8")
    return destination


def clean_temporary_directory() -> None:
    """
    Ensure the temporary directory exists.

    This helper is included so downstream jobs can rely on a known temp location
    even before preprocessing is added.
    """
    ensure_directory(TMP_DIR)


def ingest_trip_months_from_iterable(
    months: Iterable[str],
    overwrite: bool = False,
) -> list[dict]:
    """
    Ingest an arbitrary iterable of YYYY-MM month strings.
    """
    session = create_session()
    manifests: list[dict] = []

    for month_str in months:
        month = parse_month(month_str)
        manifests.append(ingest_trip_month(session=session, month=month, overwrite=overwrite))

    return manifests