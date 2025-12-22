"""Pytest fixtures for testing."""

import pytest
import sqlite3
import shutil
from pathlib import Path


@pytest.fixture(scope="session")
def test_db_path(tmp_path_factory):
    """Create temporary copy of database for testing."""
    project_root = Path(__file__).parent.parent
    source_db = project_root / "product_sales.db"
    
    tmp_dir = tmp_path_factory.mktemp("data")
    test_db = tmp_dir / "test_product_sales.db"
    
    shutil.copy(source_db, test_db)
    return str(test_db)


@pytest.fixture(scope="function")
def db_connection(test_db_path):
    """Provide database connection for each test."""
    conn = sqlite3.connect(test_db_path)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def clean_revenue_table(db_connection):
    """Ensure revenue table is clean before each test."""
    cursor = db_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS revenue")
    db_connection.commit()
    yield
    cursor.execute("DROP TABLE IF EXISTS revenue")
    db_connection.commit()
