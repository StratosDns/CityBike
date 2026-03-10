from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any


def ensure_directory(path: Path) -> Path:
    """
    Create a directory and all missing parents if needed.

    Returns:
        The same path that was passed in.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def configure_logging(log_file: Path | None = None, level: int = logging.INFO) -> None:
    """
    Configure console logging and, optionally, file logging.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file is not None:
        ensure_directory(log_file.parent)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )


def write_json(data: dict[str, Any], output_path: Path) -> None:
    """
    Write a JSON file with stable formatting.
    """
    ensure_directory(output_path.parent)
    output_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")