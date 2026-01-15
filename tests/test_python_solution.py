"""
Tests for the Python solution.
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

def test_calculate_revenue():
    product_dates = pd.DataFrame({
        'sku_id': [1, 1],
        'date_id': ['2025-01-01', '2025-01-02'],
        'price': [10.0, 10.0]
    })
    
    sales = pd.DataFrame({
        'sku_id': [1],
        'date_id': ['2025-01-01'],
        'sales': [5]
    })
    
    result = calculate_revenue(product_dates, sales)
    
    assert len(result) == 2
    
    # 10.0 × 5 = 50.0
    row = result[(result['sku_id'] == 1) & (result['date_id'] == '2025-01-01')]
    assert row['revenue'].values[0] == 50.0
    
    # No sales on 2025-01-02, should be 0
    row = result[(result['sku_id'] == 1) & (result['date_id'] == '2025-01-02')]
    assert row['sales'].values[0] == 0
    assert row['revenue'].values[0] == 0.0


def test_main_pipeline(test_db_path, clean_revenue_table):
    main(test_db_path)
    
    conn = sqlite3.connect(test_db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='revenue'")
    assert cursor.fetchone() is not None
    
    cursor.execute("SELECT COUNT(*) FROM revenue")
    row_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM product")
    product_count = cursor.fetchone()[0]
    
    expected_rows = product_count * 31
    assert row_count == expected_rows
    
    conn.close()


def test_python_vs_sql_same_results(test_db_path, clean_revenue_table):
    #comparing the 2 solutions results for confirmation both works same
    main(test_db_path)
    
    conn = sqlite3.connect(test_db_path)
    python_df = pd.read_sql_query(
        "SELECT sku_id, date_id, price, sales, revenue FROM revenue ORDER BY sku_id, date_id",
        conn
    )
    
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS revenue")
    conn.commit()
    
    project_root = Path(__file__).parent.parent
    sql_script = project_root / "sql" / "create_revenue_table.sql"
    
    with open(sql_script, 'r') as f:
        sql_script_content = f.read()
    
    cursor.executescript(sql_script_content)
    conn.commit()
    
    sql_df = pd.read_sql_query(
        "SELECT sku_id, date_id, price, sales, revenue FROM revenue ORDER BY sku_id, date_id",
        conn
    )
    
    conn.close()
    
    assert len(python_df) == len(sql_df)
    assert python_df['revenue'].sum() == pytest.approx(sql_df['revenue'].sum(), rel=0.01)
    assert python_df['sales'].sum() == sql_df['sales'].sum()
