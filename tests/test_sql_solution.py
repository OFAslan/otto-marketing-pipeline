"""
Tests for the SQL solution.

Following TDD and 100% line coverage principles.
"""

import pytest
from pathlib import Path


def run_sql_script(conn, script_path):
    """Helper function to run a SQL script."""
    with open(script_path, 'r') as f:
        sql_script = f.read()
    
    cursor = conn.cursor()
    cursor.executescript(sql_script)
    conn.commit()


@pytest.fixture
def revenue_table_from_sql(db_connection, clean_revenue_table):
    """Run the SQL script to create the revenue table."""
    project_root = Path(__file__).parent.parent
    sql_script = project_root / "sql" / "create_revenue_table.sql"
    
    run_sql_script(db_connection, sql_script)
    return db_connection


def test__sql_solution__with_all_products_and_dates__correct_row_count(revenue_table_from_sql):
    """Test that we have exactly products × 31 days rows."""
    cursor = revenue_table_from_sql.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM product")
    product_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM revenue")
    revenue_count = cursor.fetchone()[0]
    
    expected_count = product_count * 31
    assert revenue_count == expected_count, \
        f"Expected {expected_count} rows (products × 31 days), got {revenue_count}"


def test__sql_solution__with_no_sales__zero_revenue(revenue_table_from_sql):
    """Test that products with no sales have zero revenue."""
    cursor = revenue_table_from_sql.cursor()
    
    cursor.execute("""
        SELECT DISTINCT p.sku_id
        FROM product p
        WHERE p.sku_id NOT IN (
            SELECT DISTINCT sku_id 
            FROM sales 
            WHERE DATE(orderdate_utc) BETWEEN '2025-01-01' AND '2025-01-31'
        )
        LIMIT 1
    """)
    
    result = cursor.fetchone()
    if result:
        no_sales_sku = result[0]
        
        cursor.execute("""
            SELECT sales, revenue 
            FROM revenue 
            WHERE sku_id = ?
        """, (no_sales_sku,))
        
        rows = cursor.fetchall()
        assert len(rows) == 31, "Product should appear for all 31 days"
        
        for sales, revenue in rows:
            assert sales == 0, f"Expected 0 sales, got {sales}"
            assert revenue == 0.0, f"Expected 0.0 revenue, got {revenue}"


def test__sql_solution__with_sales_data__correct_revenue_calculation(revenue_table_from_sql):
    """Test that revenue is correctly calculated as price × sales."""
    cursor = revenue_table_from_sql.cursor()
    
    cursor.execute("""
        SELECT sku_id, date_id, price, sales, revenue
        FROM revenue
        WHERE sales > 0
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    assert len(rows) > 0, "Should have at least some rows with sales"
    
    for sku_id, date_id, price, sales, revenue in rows:
        expected_revenue = round(price * sales, 2)
        assert abs(revenue - expected_revenue) < 0.01, \
            f"Revenue mismatch for sku {sku_id} on {date_id}: " \
            f"expected {expected_revenue}, got {revenue}"


def test__sql_solution__with_multiple_sales_same_day__aggregated_correctly(revenue_table_from_sql):
    """Test that multiple sales on the same day are aggregated."""
    cursor = revenue_table_from_sql.cursor()
    
    cursor.execute("""
        SELECT sku_id, DATE(orderdate_utc) as order_date, SUM(sales) as total_sales
        FROM sales
        WHERE DATE(orderdate_utc) BETWEEN '2025-01-01' AND '2025-01-31'
        GROUP BY sku_id, DATE(orderdate_utc)
        HAVING COUNT(*) > 1
        LIMIT 1
    """)
    
    result = cursor.fetchone()
    if result:
        sku_id, order_date, expected_sales = result
        
        cursor.execute("""
            SELECT sales FROM revenue
            WHERE sku_id = ? AND date_id = ?
        """, (sku_id, order_date))
        
        actual_sales = cursor.fetchone()[0]
        assert actual_sales == expected_sales, \
            f"Expected aggregated sales {expected_sales}, got {actual_sales}"


def test__sql_solution__with_data__no_null_values(revenue_table_from_sql):
    """Test that there are no NULL values in key columns."""
    cursor = revenue_table_from_sql.cursor()
    
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN sku_id IS NULL THEN 1 ELSE 0 END) as sku_nulls,
            SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) as date_nulls,
            SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as price_nulls
        FROM revenue
    """)
    
    nulls = cursor.fetchone()
    assert sum(nulls) == 0, f"Found NULL values: {nulls}"
