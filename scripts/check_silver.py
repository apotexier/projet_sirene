"""Simple profiling script to verify the Silver layer data quality."""

import duckdb
from pathlib import Path
from sirene_pipeline.config import settings

def check_silver_data():
    """Profiles Silver Parquet files and verifies regional filters."""
    con = duckdb.connect()
    
    # Ensure the path is correctly retrieved from Dynaconf
    silver_dir = Path(settings.silver.output_dir)
    
    if not silver_dir.exists():
        print(f"❌ Error: Silver directory not found at {silver_dir}")
        return

    for parquet_file in silver_dir.glob("*_silver.parquet"):
        # Use absolute path and fix backslashes for DuckDB on Windows
        file_path = str(parquet_file.absolute()).replace("\\", "/")
        
        print(f"\n{'='*60}")
        print(f"📊 PROFILING: {parquet_file.name}")
        print(f"{'='*60}")
        
        # 1. Total count
        row_count = con.execute(f"SELECT count(*) FROM read_parquet('{file_path}')").fetchone()[0]
        print(f"📈 Total Rows: {row_count:,}")
        
        # 2. Schema & Types
        info = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
        print("\n--- Schema & Types ---")
        print(info[['column_name', 'column_type']])
        
        # 3. Quick statistics
        # Note: 'approx_unique' is the correct name for unique counts in SUMMARIZE
        stats = con.execute(f"SUMMARIZE SELECT * FROM read_parquet('{file_path}')").df()
        print("\n--- Data Statistics ---")
        # Adjusting column names to match DuckDB's SUMMARIZE output
        available_cols = ['column_name', 'null_percentage', 'approx_unique', 'min', 'max']
        print(stats[available_cols])

        # 4. Regional Check (Crucial to verify your IDF Filter)
        if "etablissements" in parquet_file.name:
            print("\n--- 📍 Regional Distribution (IDF Check) ---")
            distrib = con.execute(f"""
                SELECT 
                    substring(codePostalEtablissement, 1, 2) AS dept,
                    count(*) AS total_count,
                    round(count(*) * 100.0 / {row_count}, 2) AS percentage
                FROM read_parquet('{file_path}')
                GROUP BY dept
                ORDER BY dept
            """).df()
            print(distrib)

    con.close()

if __name__ == "__main__":
    check_silver_data()