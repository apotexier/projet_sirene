"""Script to validate the Bronze layer data quality with a clean, readable preview."""

import duckdb
import pandas as pd
from pathlib import Path
from loguru import logger

def verify_bronze_data(file_path: Path):
    """Prints statistics and a clean truncated preview of a Parquet file.
    
    Args:
        file_path: Path to the parquet file to check.
    """
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Checking: {file_path.name}")
    con = duckdb.connect()
    
    try:
        # 1. Row Count
        count = con.execute(f"SELECT count(*) FROM read_parquet('{file_path}')").fetchone()[0]
        
        # 2. Get column count
        schema_info = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").fetchall()
        num_columns = len(schema_info)
        
        # 3. Pandas Configuration - Safe & Clean
        # We only set what we need, avoiding reset_option('all')
        pd.set_option('display.max_columns', 10)  # Show 5 start and 5 end columns
        pd.set_option('display.width', 120)       # Avoid line wrapping
        pd.set_option('display.max_rows', 10)     # Keep it compact
        
        # 4. Get a 5-row preview
        preview = con.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 5").df()
        
        print("-" * 60)
        print(f"REPORT FOR {file_path.name}")
        print(f"Total rows: {count:,}")
        print(f"Number of columns: {num_columns}")
        
        print("\nFirst 5 rows preview (truncated):")
        print(preview) 
        print("-" * 60)
        
    except Exception as e:
        logger.error(f"Could not read {file_path.name}: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    bronze_dir = Path("data/bronze")
    if not bronze_dir.exists():
        logger.error(f"Directory {bronze_dir} not found. Run the pipeline first.")
    else:
        for parquet_file in bronze_dir.glob("*.parquet"):
            verify_bronze_data(parquet_file)