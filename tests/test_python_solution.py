"""
Tests for the Python solution.

Following TDD and 100% line coverage principles.
"""

import pytest
import sys
from pathlib import Path
import pandas as pd
import sqlite3

# Add python directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from revenue_pipeline import (
    extract_data,
    generate_date_dimension,
    create_product_date_cartesian,
    calculate_revenue,
    main
)


def test__generate_date_dimension__with_january_2025__returns_31_dates():
    """Test that date dimension generates all days in January."""
    dates = generate_date_dimension('2025-01-01', '2025-01-31')
    
    assert len(dates) == 31
    assert dates[0] == '2025-01-01'
    assert dates[-1] == '2025-01-31'


def test__create_product_date_cartesian__with_products_and_dates__creates_all_combinations():
    """Test that cartesian product creates all combinations."""
    products = pd.DataFrame({
        'sku_id': [1, 2, 3],
        'sku_description': ['A', 'B', 'C'],
        'price': [10.0, 20.0, 30.0]
    })
    
    dates = ['2025-01-01', '2025-01-02']
    
    result = create_product_date_cartesian(products, dates)
    
    assert len(result) == 6  # 3 products × 2 dates
    assert 'sku_id' in result.columns
    assert 'date_id' in result.columns
    assert 'price' in result.columns
    
    # Verify each product appears for each date
    for sku_id in [1, 2, 3]:
        for date in dates:
            matching = result[(result['sku_id'] == sku_id) & (result['date_id'] == date)]
            assert len(matching) == 1


def test__calculate_revenue__with_sales_data__calculates_correctly():
    """Test that revenue calculation works correctly."""
    product_dates = pd.DataFrame({
        'sku_id': [1, 1],
        'date_id': ['2025-01-01', '2025-01-02'],
        'price': [10.0, 10.0]
    })
    
    sales = pd.DataFrame({
        'sku_id': [1],
        'order_date': ['2025-01-01'],
        'sales': [5]
    })
    
    result = calculate_revenue(product_dates, sales)
    
    assert len(result) == 2
    
    # Check calculation: 10.0 × 5 = 50.0
    row = result[(result['sku_id'] == 1) & (result['date_id'] == '2025-01-01')]
    assert row['revenue'].values[0] == 50.0
    
    # No sales on 2025-01-02, should be 0
    row = result[(result['sku_id'] == 1) & (result['date_id'] == '2025-01-02')]
    assert row['sales'].values[0] == 0
    assert row['revenue'].values[0] == 0.0


def test__calculate_revenue__with_no_sales__zero_revenue():
    """Test that products with no sales have zero revenue."""
    product_dates = pd.DataFrame({
        'sku_id': [1],
        'date_id': ['2025-01-01'],
        'price': [10.0]
    })
    
    sales = pd.DataFrame(columns=['sku_id', 'order_date', 'sales'])
    
    result = calculate_revenue(product_dates, sales)
    
    assert len(result) == 1
    assert result['sales'].values[0] == 0
    assert result['revenue'].values[0] == 0.0


def test__main_pipeline__with_database__completes_successfully(test_db_path, clean_revenue_table):
    """Integration test: Run the entire pipeline."""
    # Run the main pipeline
    main(test_db_path)
    
    # Verify results
    import sqlite3
    conn = sqlite3.connect(test_db_path)
    cursor = conn.cursor()
    
    # Check table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='revenue'")
    assert cursor.fetchone() is not None
    
    # Check row count
    cursor.execute("SELECT COUNT(*) FROM revenue")
    row_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM product")
    product_count = cursor.fetchone()[0]
    
    expected_rows = product_count * 31
    assert row_count == expected_rows, f"Expected {expected_rows} rows, got {row_count}"
    
    # Check no nulls
    cursor.execute("""
        SELECT COUNT(*) FROM revenue 
        WHERE sku_id IS NULL OR date_id IS NULL OR price IS NULL
    """)
    null_count = cursor.fetchone()[0]
    assert null_count == 0
    
    conn.close()


def test__python_vs_sql__both_produce_same_results(test_db_path, clean_revenue_table):
    """Test that Python and SQL solutions produce identical results."""
    import sqlite3
    from pathlib import Path
    
    # Run Python solution
    main(test_db_path)
    
    # Get Python results
    conn = sqlite3.connect(test_db_path)
    python_df = pd.read_sql_query(
        "SELECT sku_id, date_id, price, sales, revenue FROM revenue ORDER BY sku_id, date_id",
        conn
    )
    
    # Clear table and run SQL solution
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS revenue")
    conn.commit()
    
    project_root = Path(__file__).parent.parent
    sql_script = project_root / "sql" / "create_revenue_table.sql"
    
    with open(sql_script, 'r') as f:
        sql_script_content = f.read()
    
    cursor.executescript(sql_script_content)
    conn.commit()
    
    # Get SQL results
    sql_df = pd.read_sql_query(
        "SELECT sku_id, date_id, price, sales, revenue FROM revenue ORDER BY sku_id, date_id",
        conn
    )
    
    conn.close()
    
    # Compare results
    assert len(python_df) == len(sql_df), "Row counts should match"
    
    # Compare key metrics
    assert python_df['revenue'].sum() == pytest.approx(sql_df['revenue'].sum(), rel=0.01), \
        "Total revenue should match"
    assert python_df['sales'].sum() == sql_df['sales'].sum(), \
        "Total sales should match"
