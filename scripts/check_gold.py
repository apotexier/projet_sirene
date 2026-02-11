"""Script to validate the quality and content of Gold layer files."""

import duckdb
from pathlib import Path
from loguru import logger
from sirene_pipeline.config import settings

def check_gold_outputs():
    """Reads and summarizes Gold Parquet files to ensure data quality."""
    gold_dir = Path(settings.gold.output_dir)
    
    if not gold_dir.exists():
        logger.error(f"❌ Gold directory not found: {gold_dir}")
        return

    con = duckdb.connect(database=":memory:")
    
    logger.info("🔍 Starting Gold Layer Verification...")
    print("-" * 50)

    # List of files to check (Master + KPIs)
    files_to_check = [
        settings.gold.master_filename,
        settings.gold.kpis.dept_dist,
        settings.gold.kpis.sectors,
        settings.gold.kpis.size_dist
    ]

    for filename in files_to_check:
        file_path = gold_dir / filename
        
        if not file_path.exists():
            logger.warning(f"⚠️ Missing file: {filename}")
            continue

        # Basic statistics for each file
        try:
            # Get row count and a quick preview
            stats = con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path.as_posix()}')").fetchone()[0]
            
            logger.success(f"✅ {filename}: {stats:,} rows found.")
            
            # Show a small sample for the Master table or KPIs
            if "master" in filename:
                preview = con.execute(f"""
                    SELECT siret, denominationUniteLegale, departement, age_entreprise 
                    FROM read_parquet('{file_path.as_posix()}') 
                    LIMIT 3
                """).df()
            else:
                preview = con.execute(f"SELECT * FROM read_parquet('{file_path.as_posix()}') LIMIT 3").df()
            
            print(f"\nPreview for {filename}:\n{preview}\n")
            print("-" * 50)

        except Exception as e:
            logger.error(f"❌ Error reading {filename}: {e}")

    # Specific Data Quality Check: Null values in critical joined columns
    master_path = (gold_dir / settings.gold.master_filename).as_posix()
    if Path(master_path).exists():
        null_count = con.execute(f"""
            SELECT COUNT(*) 
            FROM read_parquet('{master_path}') 
            WHERE denominationUniteLegale IS NULL AND nomUniteLegale IS NULL
        """).fetchone()[0]
        
        if null_count > 0:
            logger.warning(f"❗ Alert: {null_count:,} establishments have no matching Legal Unit (Left Join gaps).")
        else:
            logger.success("✨ Join integrity check: All establishments matched with a Legal Unit.")

    con.close()
    logger.info("🏁 Verification complete.")

if __name__ == "__main__":
    check_gold_outputs()