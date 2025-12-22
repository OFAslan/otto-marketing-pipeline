"""
Revenue Table ETL Pipeline

Purpose: Generate daily revenue data for all products in January 2025
Business Requirement: Show revenue for EVERY product on EVERY day, even if not sold

With this script I followed 3 step ETL approach:
- Extract: Load data from SQLite database
- Transform: Generate date spine, aggregate sales, calculate revenue
- Load: Write results back to database
"""

import sqlite3
import logging
from typing import List
import pandas as pd


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def extract_data(conn, query, params=None):
    """Extract data from database."""
    df = pd.read_sql_query(query, conn, params=params)

    logger.info(f"Extracted {len(df)} rows")
    return df


def generate_date_dimension(start_date: str, end_date: str):
    """Generate list of all dates."""
    logger.info(f"Generating date list from {start_date} to {end_date}...")

    dates = pd.date_range(start_date, end_date).strftime('%Y-%m-%d').tolist()
    logger.info(f"Generated {len(dates)} dates")
    return dates

def create_product_date_cartesian(products_df: pd.DataFrame, dates: List[str]) -> pd.DataFrame:
    """
    Creating a cartesian product of all products and all dates.
    This ensures every product appears for every date, even if not sold.
    Returns a df with columns: sku_id, date_id, price
    """
    logger.info("Creating cartesian products")
    
    # Create a DataFrame from dates
    dates_df = pd.DataFrame({'date_id': dates})
    
    # Add a temporary key for cartesian product
    products_df['_key'] = 1
    dates_df['_key'] = 1
    
    # Perform cartesian product
    cartesian = products_df.merge(dates_df, on='_key').drop('_key', axis=1)
    
    # Select and rename columns
    result = cartesian[['sku_id', 'date_id', 'price']]
    
    logger.info(f"Created {len(result)} product-date combinations")
    return result


def calculate_revenue(product_date_df, sales_df):
    """Calculate revenue with sales aggregation."""
    logger.info("Aggregating sales and calculating revenue...")
    
    # Aggregate sales
    if not sales_df.empty:
        sales_agg = sales_df.groupby(['sku_id', 'order_date'], as_index=False).agg({'sales': 'sum'})
    else:
        sales_agg = pd.DataFrame(columns=['sku_id', 'order_date', 'sales'])
    
    # Join and calculate
    revenue_df = product_date_df.merge(sales_agg, left_on=['sku_id', 'date_id'], 
                                       right_on=['sku_id', 'order_date'], how='left')
    revenue_df['sales'] = revenue_df['sales'].fillna(0).astype(int)
    revenue_df['revenue'] = (revenue_df['price'] * revenue_df['sales']).round(2)
    
    return revenue_df[['sku_id', 'date_id', 'price', 'sales', 'revenue']]


def load_revenue_table(conn: sqlite3.Connection, revenue_df: pd.DataFrame) -> None:
    """
    Load revenue data into the database.
    Drops and recreates the table for idempotency.
    """
    logger.info("Loading revenue data to database...")
    
    cursor = conn.cursor()
    
    # Drop existing table
    cursor.execute("DROP TABLE IF EXISTS revenue")
    logger.info("Dropped existing revenue table")
    
    # Create new table
    create_table_sql = """
        CREATE TABLE revenue (
            sku_id TEXT NOT NULL,
            date_id DATE NOT NULL,
            price REAL NOT NULL,
            sales INTEGER NOT NULL DEFAULT 0,
            revenue REAL NOT NULL DEFAULT 0.0
        )
    """
    cursor.execute(create_table_sql)
    logger.info("Created new revenue table")
    
    # Insert data
    revenue_df.to_sql('revenue', conn, if_exists='append', index=False)
    logger.info(f"Inserted {len(revenue_df)} rows into revenue table")
    
    # Create indexes for performance
    cursor.execute("CREATE INDEX idx_revenue_sku ON revenue(sku_id)")
    cursor.execute("CREATE INDEX idx_revenue_date ON revenue(date_id)")
    cursor.execute("CREATE INDEX idx_revenue_sku_date ON revenue(sku_id, date_id)")
    logger.info("Created indexes on revenue table")
    
    conn.commit()
    logger.info("Transaction committed successfully")


def validate_results(conn, expected_products, expected_dates):
    """Perform validation checks."""
    logger.info("Running validation checks...")
    cursor = conn.cursor()
    checks = []
    
    # Row count
    actual = cursor.execute("SELECT COUNT(*) FROM revenue").fetchone()[0]
    expected = expected_products * expected_dates
    checks.append(("Row count", actual == expected, f"{actual} rows"))
    
    # No NULLs
    nulls = cursor.execute("SELECT SUM(CASE WHEN sku_id IS NULL OR date_id IS NULL OR price IS NULL THEN 1 ELSE 0 END) FROM revenue").fetchone()[0]
    checks.append(("NULL check", nulls == 0, "no NULLs"))
    
    # Revenue calculation
    errors = cursor.execute("SELECT COUNT(*) FROM revenue WHERE ABS(revenue - (price * sales)) > 0.01").fetchone()[0]
    checks.append(("Revenue calc", errors == 0, "all correct"))
    
    all_passed = all(passed for _, passed, _ in checks)
    for name, passed, msg in checks:
        logger.info(f"{name}: {'PASS' if passed else 'FAIL'} - {msg}")
    
    return all_passed


def main(db_path: str = 'product_sales.db'):
    """
    Main pipeline orchestration function.
    """
    logger.info("=" * 60)
    logger.info("Starting Revenue Pipeline")
    logger.info("=" * 60)
    
    try:
        # Configuration (January 2025)
        START_DATE = '2025-01-01'
        END_DATE = '2025-01-31'
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to database: {db_path}")
        
        # EXTRACT
        logger.info("\n--- EXTRACT PHASE ---")
        products_df = extract_data(conn, "SELECT sku_id, sku_description, price FROM product")
        sales_df = extract_data(conn, 
            "SELECT sku_id, DATE(orderdate_utc) as order_date, sales FROM sales WHERE DATE(orderdate_utc) BETWEEN ? AND ?",
            (START_DATE, END_DATE))
        
        # TRANSFORM
        logger.info("\n--- TRANSFORM PHASE ---")
        dates = generate_date_dimension(START_DATE, END_DATE)
        product_date_spine = create_product_date_cartesian(products_df, dates)
        revenue_df = calculate_revenue(product_date_spine, sales_df)
        
        # LOAD
        logger.info("\n--- LOAD PHASE ---")
        load_revenue_table(conn, revenue_df)
        
        # VALIDATE
        logger.info("\n--- VALIDATION PHASE ---")
        validation_passed = validate_results(conn, len(products_df), len(dates))
        
        # Close connection
        conn.close()
        logger.info("Database connection closed")
        
        # Final status
        logger.info("\n" + "=" * 60)
        if validation_passed:
            logger.info("Pipeline completed successfully!")
        else:
            logger.warning("Pipeline completed with validation warnings")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
