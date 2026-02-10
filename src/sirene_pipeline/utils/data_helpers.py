"""Utility functions for data tracking and watermarking."""

from datetime import datetime
from pathlib import Path

import duckdb


def get_last_ingested_date(silver_path: Path) -> datetime:
    """Finds the maximum ingested_at date in the existing Silver file.

    This function uses DuckDB to efficiently query the metadata of the
    Parquet file without loading the entire dataset into memory.

    Args:
        silver_path: Path to the Silver parquet file.

    Returns:
        The maximum datetime found in the 'ingested_at' column,
        or a default old date if the file does not exist.
    """
    if not silver_path.exists():
        return datetime(1900, 1, 1)

    con = duckdb.connect()
    try:
        # We query the max date directly from the parquet file
        result = con.execute(
            f"SELECT MAX(ingested_at) FROM read_parquet('{silver_path}')"
        ).fetchone()

        last_date = result[0] if result else None

        # Ensure we return a datetime object, not None
        return last_date if last_date else datetime(1900, 1, 1)
    except Exception:
        # In case the column doesn't exist yet or file is corrupted
        return datetime(1900, 1, 1)
    finally:
        con.close()
