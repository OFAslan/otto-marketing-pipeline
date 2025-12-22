/*
 * Data Quality Validation Queries for Revenue Table
 * These should be run after creating the revenue table to verify correctness
 */

-- Check 1: Row count (should be number_of_products × 31 days)
SELECT 
    'Row count' AS check_name,
    COUNT(*) AS actual_rows,
    (SELECT COUNT(*) FROM product) * 31 AS expected_rows,
    CASE 
        WHEN COUNT(*) = (SELECT COUNT(*) FROM product) * 31 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status
FROM revenue;

-- Check 2: No NULL values in key columns
SELECT 
    'NULL check' AS check_name,
    SUM(CASE WHEN sku_id IS NULL OR date_id IS NULL OR price IS NULL THEN 1 ELSE 0 END) AS null_count,
    CASE 
        WHEN SUM(CASE WHEN sku_id IS NULL OR date_id IS NULL OR price IS NULL THEN 1 ELSE 0 END) = 0 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status
FROM revenue;

-- Check 3: Revenue calculation is correct (revenue = price × sales)
SELECT 
    'Revenue calc' AS check_name,
    SUM(CASE WHEN ABS(revenue - (price * sales)) > 0.01 THEN 1 ELSE 0 END) AS incorrect_calculations,
    CASE 
        WHEN SUM(CASE WHEN ABS(revenue - (price * sales)) > 0.01 THEN 1 ELSE 0 END) = 0 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status
FROM revenue;