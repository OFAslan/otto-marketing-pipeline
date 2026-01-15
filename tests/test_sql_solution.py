"""
Tests for the SQL solution.
"""

import pytest
from pathlib import Path


def run_sql_script(conn, script_path):
    #Helper function to run a SQL script.
    with open(script_path, 'r') as f:
        sql_script = f.read()
    
    cursor = conn.cursor()
    cursor.executescript(sql_script)
    conn.commit()



@pytest.fixture
def revenue_table_from_sql(db_connection, clean_revenue_table):
    project_root = Path(__file__).parent.parent
    sql_script = project_root / "sql" / "create_revenue_table.sql"
    run_sql_script(db_connection, sql_script)
    return db_connection


def test_sql_row_count(revenue_table_from_sql):
    cursor = revenue_table_from_sql.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM product")
    product_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM revenue")
    revenue_count = cursor.fetchone()[0]
    
    expected_count = product_count * 31
    assert revenue_count == expected_count


def test_sql_revenue_calculation(revenue_table_from_sql):
    cursor = revenue_table_from_sql.cursor()
    
    cursor.execute("""
        SELECT sku_id, date_id, price, sales, revenue
        FROM revenue
        WHERE sales > 0
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    assert len(rows) > 0
    
    for sku_id, date_id, price, sales, revenue in rows:
        expected_revenue = round(price * sales, 2)
        assert abs(revenue - expected_revenue) < 0.01