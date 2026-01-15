# Revenue Table ETL Pipeline

A production-ready data pipeline for generating a daily revenue report. This solution provides both SQL and Python implementations to create a `revenue` table showing financial data for EVERY product on EVERY day in January 2025, including products with no sales.

## Business Context

The marketing department requires a PowerBI visualization showing daily revenue for all products in January 2025. The key requirement is to display **every product for every day**, even if the product had zero sales on that day.

## Solution Overview

Both implementations follow the same ETL approach:

1. **Extract**: Load product and sales data from the database
2. **Transform**: 
   - Generate a date spine for all days in January 2025
   - Create a cartesian product of all products × all dates
   - Aggregate sales by product and date
   - Calculate revenue (price × sales)
3. **Load**: Write results to the `revenue` table

- **SQL**: Demonstrates database-native ETL, efficient for BigQuery (The current version also can run in **SQLite**)
- **Python**: Shows structured programming, suitable for Airflow DAGs

## Project Structure

```
otto-marketing-pipeline/
├── README.md                      # This file
├── requirements.txt               # Python dependencies
├── product_sales.db              # SQLite database
├── sql/
│   ├── create_revenue_table.sql  # SQL solution
├── python/
│   └── revenue_pipeline.py       # Python solution
└── tests/
    ├── conftest.py               # Test fixtures
    ├── test_sql_solution.py      # SQL tests
    └── test_python_solution.py   # Python tests
```

### Prerequisites to run the scripts

- Python 3.9+
- SQLite3

### Setup

```bash
# Clone or navigate to the project directory
cd otto-marketing-pipeline
```

#### Run SQL Solution

```bash
# Execute the SQL script
sqlite3 product_sales.db < sql/create_revenue_table.sql

#### Run Python Solution

```bash
# Install dependencies (first time only)
pip install -r requirements.txt

# Run the pipeline
cd python
python revenue_pipeline.py
```

### Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test suite
pytest tests/test_sql_solution.py -v
pytest tests/test_python_solution.py -v
```

### Test Coverage

**SQL Tests:**
- Row count verification (products × 31 days)
- Revenue calculation correctness (price × sales)

**Python Tests:**
- Revenue calculation with sales data
- Revenue calculation with no sales (zero revenue)
- End-to-end pipeline integration
- SQL vs Python result comparison

## Data Quality

Both solutions include validation checks:

1. **Row Count**: Verifies total rows = products × 31 days
2. **NULL Check**: No NULL values in the key columns
3. **Revenue Calculation**: Validates revenue calculation = price × sales

### Future Enhancements can be considered for production

- Implement incremental loading
- Create Airflow DAG wrapper
- Add performance benchmarks
- Implement data quality / SLI alerts

---
Author: Oğuzhan Furkan Aslan
