-- 1. average price by year
SELECT year,
       AVG(ron95_budi95) AS avg_ron95_budi95
FROM fact_fuel_price
GROUP BY year
ORDER BY year;

-- 2. record count by year
SELECT year,
       COUNT(*) AS total_records
FROM fact_fuel_price
GROUP BY year
ORDER BY year;

-- 3. null check
SELECT 
    COUNT(*) AS total_rows,
    COUNT(ron95_budi95) AS non_null_ron95_budi95,
    COUNT(ron95_skps) AS non_null_ron95_skps
FROM fact_fuel_price;

-- 4. maximum price
SELECT MAX(ron95_budi95) AS highest_price
FROM fact_fuel_price;

-- 5. minimum price
SELECT MIN(ron95_budi95) AS lowest_price
FROM fact_fuel_price;