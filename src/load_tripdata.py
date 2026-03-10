from __future__ import annotations

import argparse
import logging
from pathlib import Path

from citibike_loader import ingest_trip_range, ingest_trip_months_from_iterable
from config import LOG_DIR
from utils import configure_logging


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for trip-data ingestion.
    """
    parser = argparse.ArgumentParser(
        description="Download and extract official Citi Bike monthly trip-history archives."
    )

    month_group = parser.add_mutually_exclusive_group(required=True)
    month_group.add_argument(
        "--months",
        nargs="+",
        help="One or more specific months in YYYY-MM format.",
    )
    month_group.add_argument(
        "--start-month",
        help="Inclusive start month in YYYY-MM format.",
    )

    parser.add_argument(
        "--end-month",
        help="Inclusive end month in YYYY-MM format. Required when using --start-month.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Redownload archives and re-extract files even if local copies exist.",
    )

    return parser.parse_args()


def main() -> None:
    """
    Entry point for the trip-data loader.
    """
    configure_logging(log_file=LOG_DIR / "load_tripdata.log", level=logging.INFO)
    args = parse_args()

    if args.start_month and not args.end_month:
        raise ValueError("--end-month is required when using --start-month")

    if args.months:
        manifests = ingest_trip_months_from_iterable(
            months=args.months,
            overwrite=args.overwrite,
        )
    else:
        manifests = ingest_trip_range(
            start_month=args.start_month,
            end_month=args.end_month,
            overwrite=args.overwrite,
        )

    logging.getLogger(__name__).info("Ingestion completed for %s month(s).", len(manifests))


if __name__ == "__main__":
    main()