WITH RegionalSales AS (
    SELECT 
        region,
        product_category,
        SUM(sales_amount) AS total_sales,
        COUNT(order_id) AS total_orders
    FROM 
        sales_data
    WHERE 
        TO_TIMESTAMP(order_date, 'yyyymmddhh24miss') BETWEEN TO_TIMESTAMP(%DATESTART%) AND TO_TIMESTAMP(%DATEEND%)  -- Tokens are replaced by the Airflow DAG with Start and End dates

    GROUP BY 
        region, product_category
),
TopRegions AS (
    SELECT 
        region,
        RANK() OVER (ORDER BY SUM(total_sales) DESC) AS rank
    FROM 
        RegionalSales
    GROUP BY 
        region
)
SELECT 
    rs.region,
    rs.product_category,
    rs.total_sales,
    rs.total_orders,
    tr.rank AS region_rank
FROM 
    RegionalSales rs
JOIN 
    TopRegions tr
ON 
    rs.region = tr.region
WHERE 
    tr.rank <= 5
ORDER BY 
    tr.rank, rs.total_sales DESC;