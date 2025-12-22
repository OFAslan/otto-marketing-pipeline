/*
 * Revenue Table ETL Pipeline
 * 
 * Purpose: Generate daily revenue data for all products in January 2025
 * Business Requirement: Show revenue for EVERY product on EVERY day, even if not sold
 * 
 * Steps I followed:
 * 1. Generate a date spine for all days in January 2025 using recursive CTE
 * 2. Create cartesian product of all products × all dates
 * 3. Aggregate sales data by product and date
 * 4. Join to calculate revenue (price × sales, or 0 if no sales)
 */

-- Drop and recreate the revenue table for idempotency
DROP TABLE IF EXISTS revenue;

CREATE TABLE revenue (
    sku_id TEXT NOT NULL,
    date_id DATE NOT NULL,
    price REAL NOT NULL,
    sales INTEGER NOT NULL DEFAULT 0,
    revenue REAL NOT NULL DEFAULT 0.0
);

-- Populate the revenue table
INSERT INTO revenue (sku_id, date_id, price, sales, revenue)
WITH RECURSIVE 
    -- Generate all dates in January 2025
    date_spine AS (
        SELECT DATE('2025-01-01') AS date_id
        UNION ALL
        SELECT DATE(date_id, '+1 day')
        FROM date_spine
        WHERE date_id < DATE('2025-01-31')
    ),
    
    -- Aggregate sales by product and date
    daily_sales AS (
        SELECT 
            sku_id,
            DATE(orderdate_utc) AS date_id,
            SUM(sales) AS total_sales
        FROM sales
        WHERE DATE(orderdate_utc) BETWEEN '2025-01-01' AND '2025-01-31'
        GROUP BY sku_id, DATE(orderdate_utc)
    ),
    
    -- Create cartesian product: all products × all dates
    product_date_spine AS (
        SELECT 
            p.sku_id,
            d.date_id,
            p.price
        FROM product p
        CROSS JOIN date_spine d
    )

-- Final join and revenue calculation
SELECT 
    pds.sku_id,
    pds.date_id,
    pds.price,
    COALESCE(ds.total_sales, 0) AS sales,
    ROUND(pds.price * COALESCE(ds.total_sales, 0), 2) AS revenue
FROM product_date_spine pds
LEFT JOIN daily_sales ds 
    ON pds.sku_id = ds.sku_id 
    AND pds.date_id = ds.date_id
ORDER BY pds.sku_id, pds.date_id;

-- Create indexes for query performance
CREATE INDEX idx_revenue_sku ON revenue(sku_id);
CREATE INDEX idx_revenue_date ON revenue(date_id);
CREATE INDEX idx_revenue_sku_date ON revenue(sku_id, date_id);
