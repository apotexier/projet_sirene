"""Script to verify the integrity and content of the Silver layer."""

from pathlib import Path

import pandas as pd
from loguru import logger

from sirene_pipeline.config import settings


def check_silver_layer() -> None:
    """Analyzes Silver files to verify incremental loads and feature engineering."""
    silver_dir = Path(settings.silver.output_dir)

    if not silver_dir.exists():
        logger.error(f"❌ Silver directory not found at {silver_dir}")
        return

    silver_files = list(silver_dir.glob("*.parquet"))

    if not silver_files:
        logger.warning("⚠️ No parquet files found in Silver directory.")
        return

    for file_path in silver_files:
        logger.info(f"🔍 Analyzing: {file_path.name}")
        df = pd.read_parquet(file_path)

        # 1. Basic Stats
        total_rows = len(df)
        logger.info(f"📊 Total records: {total_rows:,}")

        # 2. Check New Engineered Columns
        new_cols = ["activity_sector", "company_age", "ingested_at"]

        # Only check department for establishments
        if "etablissements" in file_path.name:
            new_cols.append("department")

        for col in new_cols:
            if col in df.columns:
                null_pct = df[col].isna().mean() * 100
                logger.success(f"✅ Column '{col}' present (Nulls: {null_pct:.2f}%)")
            else:
                logger.error(f"❌ Column '{col}' is MISSING")

        # 3. Incremental Insight
        if "ingested_at" in df.columns:
            last_load = df["ingested_at"].max()
            first_load = df["ingested_at"].min()
            load_count = df["ingested_at"].nunique()
            logger.info(f"⏱️ Data spans from {first_load} to {last_load}")
            logger.info(f"🔄 Number of distinct ingestion batches: {load_count}")

        # 4. Business Logic Preview
        if "company_age" in df.columns:
            avg_age = df[df["company_age"] > 0]["company_age"].mean()
            logger.info(f"🏢 Average company age: {avg_age:.1f} years")

        if "department" in df.columns:
            top_depts = df["department"].value_counts().head(3).to_dict()
            logger.info(f"📍 Top 3 departments: {top_depts}")

        print("-" * 50)


if __name__ == "__main__":
    check_silver_layer()
