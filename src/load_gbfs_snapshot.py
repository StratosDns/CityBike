from __future__ import annotations

import argparse
import logging

from citibike_loader import snapshot_gbfs_feed
from config import LOG_DIR
from utils import configure_logging


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for GBFS snapshot ingestion.
    """
    parser = argparse.ArgumentParser(
        description="Download one or more official Citi Bike GBFS feeds as bronze-layer snapshots."
    )
    parser.add_argument(
        "--feeds",
        nargs="+",
        required=True,
        help=(
            "One or more GBFS feed names, for example: "
            "station_information station_status system_information"
        ),
    )
    parser.add_argument(
        "--language",
        default="en",
        help="GBFS language code. Defaults to 'en'.",
    )
    return parser.parse_args()


def main() -> None:
    """
    Entry point for the GBFS snapshot loader.
    """
    configure_logging(log_file=LOG_DIR / "load_gbfs_snapshot.log", level=logging.INFO)
    args = parse_args()

    for feed_name in args.feeds:
        snapshot_path = snapshot_gbfs_feed(feed_name=feed_name, language=args.language)
        logging.getLogger(__name__).info("Saved GBFS snapshot: %s", snapshot_path)


if __name__ == "__main__":
    main()