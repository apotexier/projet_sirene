"""Script to validate the quality and content of Gold layer files."""

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from loguru import logger

from sirene_pipeline.config import settings


def check_gold_outputs() -> None:
    """Reads and summarizes Gold Parquet files.

    This function verifies data quality, availability, and join integrity
    between establishments and legal units.
    """
    # Resolve Gold output directory from project settings
    gold_dir: Path = Path(settings.gold.output_dir)

    if not gold_dir.exists():
        logger.error(f"❌ Gold directory not found: {gold_dir}")
        return

    con: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")

    logger.info("🔍 Starting Gold layer verification...")
    print("-" * 50)

    files_to_check: list[str] = [
        settings.gold.master_filename,
        settings.gold.kpis.dept_dist,
        settings.gold.kpis.sectors,
        settings.gold.kpis.size_dist,
    ]

    for filename in files_to_check:
        file_path: Path = gold_dir / filename

        if not file_path.exists():
            logger.warning(f"⚠️ Missing file: {filename}")
            continue

        try:
            result: tuple[Any, ...] | None = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{file_path.as_posix()}')"
            ).fetchone()

            if result is None:
                logger.error(f"❌ Failed to fetch row count for {filename}")
                continue

            stats: Any = result[0]
            logger.success(f"✅ {filename}: {stats:,} rows found.")

            preview: pd.DataFrame
            if "master" in filename:
                preview = con.execute(
                    f"""
                    SELECT siret, denominationUniteLegale, departement, age_entreprise
                    FROM read_parquet('{file_path.as_posix()}')
                    LIMIT 3
                    """
                ).df()
            else:
                preview = con.execute(
                    f"SELECT * FROM read_parquet('{file_path.as_posix()}') LIMIT 3"
                ).df()

            print(f"\nPreview for {filename}:\n{preview}\n")
            print("-" * 50)

        except Exception as e:
            logger.error(f"❌ Error reading {filename}: {e}")

    # Join integrity check
    master_path: Path = gold_dir / settings.gold.master_filename
    if master_path.exists():
        res_integrity: tuple[Any, ...] | None = con.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{master_path.as_posix()}')
            WHERE denominationUniteLegale IS NULL AND nomUniteLegale IS NULL
            """
        ).fetchone()

        if res_integrity is not None:
            null_count: Any = res_integrity[0]
            if null_count > 0:
                logger.warning(f"❗ Alert: {null_count:,} records with no match.")
            else:
                logger.success("✨ Join integrity check passed.")

    con.close()
    logger.info("🏁 Verification complete.")


if __name__ == "__main__":
    check_gold_outputs()
