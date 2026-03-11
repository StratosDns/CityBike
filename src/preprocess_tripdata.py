from __future__ import annotations

import argparse
import logging

from citibike_loader import month_range
from config import LOG_DIR
from preprocessing import discover_available_months, preprocess_months
from utils import configure_logging


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for preprocessing bronze Citi Bike data into silver parquet.
    """
    parser = argparse.ArgumentParser(
        description="Preprocess bronze Citi Bike trip data into analytics-ready silver parquet files."
    )

    month_group = parser.add_mutually_exclusive_group(required=False)
    month_group.add_argument(
        "--months",
        nargs="*",
        help="Specific months in YYYY-MM format.",
    )
    month_group.add_argument(
        "--start-month",
        help="Inclusive start month in YYYY-MM format.",
    )

    parser.add_argument(
        "--end-month",
        help="Inclusive end month in YYYY-MM format. Required when using --start-month.",
    )

    return parser.parse_args()


def resolve_months(args: argparse.Namespace) -> list[str]:
    """
    Resolve the month selection from CLI arguments.
    """
    if args.start_month:
        if not args.end_month:
            raise ValueError("--end-month is required when using --start-month")

        return [month.yyyymm[:4] + "-" + month.yyyymm[4:] for month in month_range(args.start_month, args.end_month)]

    if args.months:
        return args.months

    discovered = discover_available_months()
    if not discovered:
        raise FileNotFoundError("No bronze months were found to preprocess.")

    return discovered


def main() -> None:
    """
    Entry point for preprocessing.
    """
    configure_logging(log_file=LOG_DIR / "preprocess_tripdata.log", level=logging.INFO)
    args = parse_args()
    months = resolve_months(args)

    summaries = preprocess_months(months)
    logging.getLogger(__name__).info("Finished preprocessing %s month(s).", len(summaries))


if __name__ == "__main__":
    main()