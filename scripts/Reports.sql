/*
==================================================================================================================================================
CUSTOMER REPORT
==================================================================================================================================================
Purpose:
	-This customer report consolidate key customer metrics and behavoiur
    
Highlights:
	1. Gather essential field such as names, ages, and transaction details.
    2. Segment customers into categories (VIP, Regular, and New) and age group.
    3. Aggregate customer-level metrics.
		- Total orders
        - Total Sales
        - Total quatity purchased
        - Total Products
        - Lifespan (in months)
	4. Calcualte valuable KPIs
		- recency (months since last order)
        - Average order value
        - Average monthly spend
==============================================================================================================================================*/

CREATE VIEW Report_product AS
WITH Base_query AS( 
-- Base Query: Retrieves core columns from table
	SELECT 
		f.Order_number, 
		f.product_key,  
		f.Order_date, 
		f.Sales, 
		f.Quantity, 
		c.customer_Key, 
		c.customer_number, 
		concat(c.First_name, " ",c.Last_name) AS Customer_name, 
		timestampdiff(Year,c.Birth_date, now()) AS Age
	FROM gold_fact_sales f  
	LEFT JOIN gold_dim_customer c
		ON c.customer_key = f.customer_key
	WHERE Order_date IS NOT NULL),
Customer_aggregation AS(
-- Customer Aggregation: summarizes key metrics at the customer level
SELECT 
	customer_Key, 
	customer_number, 
	Customer_name, 
	Age,
    Count(Distinct Order_number) AS Total_orders,
    sum(sales) AS Total_sales,
    sum(quantity) AS Total_quantity,
    Count(distinct product_key) AS Total_product,
    MAX(order_date) AS Last_order_date,
    timestampdiff(Month, MIN(order_date), MAX(order_date)) AS Lifespan
FROM Base_query
GROUP BY customer_Key, customer_number, 
	     Customer_name, Age)
SELECT 
	customer_Key, 
	customer_number, 
	Customer_name, 
	Age,
    CASE 
		WHEN Age < 20 THEN "below 20"
		WHEN Age BETWEEN 20 AND 29 THEN "20-29"
        WHEN Age BETWEEN 30 AND 39 THEN "30-39"
        WHEN Age BETWEEN 40 AND 49 THEN "40-49"
	ELSE "50 and Above"
	END Age_group,
    CASE 
		WHEN Lifespan >= 12 AND Total_sales > 5000 THEN "VIP"
		WHEN Lifespan >= 12 AND Total_sales <= 5000 THEN "Regular"
	ELSE "New"
	END customer_segment,
    Last_order_date,
    timestampdiff(Month, Last_order_date, now()) AS Recency,
    Total_orders
    Total_sales,
    Total_quantity,
    Total_product,
    Lifespan,
-- Compute average order value (AVO)
    CASE 
		WHEN Total_orders = 0 THEN 0
	ELSE Total_sales / Total_orders
     END AS Avg_order_value,
-- compute avarage monthly spend
    CASE 
		WHEN lifespan = 0 THEN Total_sales
	ELSE Total_sales / lifespan
     END AS Avg_monthly_spend 
FROM Customer_aggregation;

SELECT * FROM Report_customers;

 /*
==================================================================================================================================================
PRODUCT REPORT
==================================================================================================================================================
Purpose:
	-This product report consolidate key customer metrics and behavoiur
    
Highlights:
	1. Gather essential field such as product_name, categories, sub_category and transaction details.
    2. Segment products by revenue to identify High performance, mid-range and low-range.
    3. Aggregate customer-level metrics.
		- Total orders
        - Total Sales
        - Total quatity sold
        - Total customer (unique)
        - Lifespan (in months)
	4. Calcualte valuable KPIs
		- recency (months since last order)
        - Average order revenue
        - Average monthly revenue
==============================================================================================================================================*/

CREATE VIEW report_products AS

WITH base_query AS (
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products
---------------------------------------------------------------------------*/
    SELECT
	    f.order_number,
        f.order_date,
		f.customer_key,
        f.sales,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.sub_category,
        p.cost
    FROM gold_fact_sales f
    LEFT JOIN gold_dim_product p
        ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL  -- only consider valid sales dates
),

product_aggregations AS (
/*---------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------*/
SELECT
    product_key,
    product_name,
    category,
    sub_category,
    cost,
    timestampdiff(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
    MAX(order_date) AS last_sale_date,
    COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers,
    SUM(sales) AS total_sales,
    SUM(quantity) AS total_quantity,
	ROUND(AVG(CAST(sales AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
FROM base_query

GROUP BY
    product_key,
    product_name,
    category,
    sub_category,
    cost
)

/*---------------------------------------------------------------------------
  3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/
SELECT 
	product_key,
	product_name,
	category,
	sub_category,
	cost,
	last_sale_date,
	timestampdiff(MONTH, last_sale_date, now()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- Average Order Revenue (AOR)
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_revenue,

	-- Average Monthly Revenue
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales / lifespan
	END AS avg_monthly_revenue

FROM product_aggregations;

SELECT * FROM Report_products       
         












