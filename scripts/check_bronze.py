"""Simple profiling script to verify the Silver layer data quality and regional filtering."""

from pathlib import Path

import duckdb

from sirene_pipeline.config import settings


def check_silver_data() -> None:
    """Profiles Silver Parquet files and verifies regional filters."""
    con = duckdb.connect()

    # Ensure the path is correctly retrieved from Dynaconf
    silver_dir = Path(settings.silver.output_dir)

    if not silver_dir.exists():
        print(f"❌ Error: Silver directory not found at {silver_dir}")
        return

    for parquet_file in silver_dir.glob("*_silver.parquet"):
        # Normalize path for DuckDB (handles Windows backslashes)
        file_path = str(parquet_file.absolute()).replace("\\", "/")

        print(f"\n{'=' * 60}")
        print(f"📊 PROFILING: {parquet_file.name}")
        print(f"{'=' * 60}")

        # 1. Row count
        result = con.execute(f"SELECT count(*) FROM read_parquet('{file_path}')").fetchone()
        count = result[0] if result else 0
        print(f"📈 Total Rows: {count:,}")

        # 2. Schema & Types
        info = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
        print("\n--- Schema & Types ---")
        print(info[["column_name", "column_type"]])

        # 3. Data Statistics (using DuckDB SUMMARIZE)
        stats = con.execute(f"SUMMARIZE SELECT * FROM read_parquet('{file_path}')").df()
        print("\n--- Data Quality (Nulls & Uniqueness) ---")
        print(stats[["column_name", "null_percentage", "unique_count", "min", "max"]])

        # 4. Regional Check (Specific for etablissements)
        if "etablissements" in parquet_file.name:
            print("\n--- 📍 Regional Distribution (IDF Check) ---")
            # We extract the first 2 digits of the postal code to verify filtering
            distrib = con.execute(f"""
                SELECT 
                    substring(codePostalEtablissement, 1, 2) AS dept,
                    count(*) AS count,
                    round(count(*) * 100.0 / {count}, 2) AS percentage
                FROM read_parquet('{file_path}')
                GROUP BY dept
                ORDER BY dept
            """).df()
            print(distrib)

    con.close()


if __name__ == "__main__":
    check_silver_data()
