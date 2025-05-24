-- ADVANCE DATA ANALYTICS

-- CHANGE OVER TIME [Trends]
-- Analyze sales performance overtime

-- Year with the highest sales
SELECT 
    YEAR(Order_date) AS order_year, 
    Sum(Sales) AS Total_sales,
	count(DISTINCT customer_Key) AS Total_count, 
	Sum(Quantity) AS Total_quantity
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(Order_date)
ORDER BY YEAR(Order_date);

-- Month with the highest sales
SELECT 
    MONTH(Order_date) AS order_month, 
    CASE MONTH(Order_date)  
		WHEN 1 THEN "JAN"
        WHEN 2 THEN "FEB"
        WHEN 3 THEN "MAR"
        WHEN 4 THEN "APR"
        WHEN 5 THEN "MAY"
        WHEN 6 THEN "JUN"
        WHEN 7 THEN "JUL"
        WHEN 8 THEN "AUG"
        WHEN 9 THEN "SEPT"
        WHEN 10 THEN "OCT"
        WHEN 11 THEN "NOV"
	ELSE "DEC"
    END `MONTH`,
    Sum(Sales) AS Total_sales,
	count(DISTINCT customer_Key) AS Total_count, 
	Sum(Quantity) AS Total_quantity
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY  MONTH(Order_date), `MONTH`
ORDER BY  MONTH(Order_date);

-- How many customer where added each year
SELECT 
    YEAR(create_date) AS create_year, 
	count(customer_Key) AS Total_customer
FROM gold_dim_customer
WHERE create_date IS NOT NULL
GROUP BY YEAR(create_date)
ORDER BY YEAR(create_date);

-- CUMMULATIVE ANALYSIS
-- Analyze sales performance overtime
-- Calculate the total sales by month and the running total of sales over time
SELECT 
order_date,
Total_sales,
Sum(Total_sales) OVER(PARTITION BY Order_date ORDER BY order_date) AS running_total_sales,
Round(AVG(Average_price) OVER(PARTITION BY Order_date ORDER BY order_date)) AS moving_average
FROM
	(SELECT 
		DATE(Order_date) AS order_date, 
		Sum(Sales) AS Total_sales,
		AVG(price) AS Average_price
	FROM gold_fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATE(Order_date)) t;

/* Analyze the yearly performnace of product by comparing their sales to both it's 
average sales performance and the previous year sales*/
WITH Yearly_product_sales AS (
	SELECT
		YEAR(f.Order_date) AS order_date, 
		p.Product_name,
		Sum(f.Sales) AS current_sales
	FROM gold_fact_sales f  
	LEFT JOIN gold_dim_product p
		ON p.product_key = f.product_key
	WHERE f.Order_date IS NOT NULL
	GROUP BY YEAR(f.Order_date), p.product_name)
SELECT 
	order_date,
    product_name,
    current_sales,
    Round(AVG(current_sales) OVER(PARTITION BY product_name)) AS avg_sales,
    current_sales - Round(AVG(current_sales) OVER(PARTITION BY product_name)) AS diff_avg,
    CASE 
		WHEN current_sales - Round(AVG(current_sales) OVER(PARTITION BY product_name)) > 0 THEN "above average"
        WHEN current_sales - Round(AVG(current_sales) OVER(PARTITION BY product_name)) < 0 THEN "Below average"
	ELSE "Average"
    END Average_change,
    -- Year-Over-year Analysis
    Lag(current_sales) OVER(PARTITION BY product_name ORDER BY order_date) Py_sales,
    current_sales - Lag(current_sales) OVER(PARTITION BY product_name ORDER BY order_date) diff_Py,
    CASE 
		WHEN current_sales - Lag(current_sales) OVER(PARTITION BY product_name ORDER BY order_date) > 0 THEN "Increase"
        WHEN current_sales - Lag(current_sales) OVER(PARTITION BY product_name ORDER BY order_date) < 0 THEN "Decrease"
	ELSE "No change"
    END Py_change
FROM Yearly_product_sales
ORDER BY product_name, order_date;

-- Which categories contribute the most to the overall sales
WITH category_sales AS
	(SELECT
		p.category AS category,
		Sum(f.Sales) AS Total_sales
	FROM  gold_fact_sales f   
	LEFT JOIN gold_dim_product p 
		ON p.product_key = f.product_key
	GROUP BY p.category)
SELECT
	category,
	Total_sales,
    sum(Total_sales) OVER() AS overall_sales,
    Concat((Total_sales / sum(Total_sales) OVER()) * 100, "%") AS per_of_total
FROM category_sales
ORDER BY per_of_total DESC;

-- Segment products into cost ranges and count how many products fall into each segment 
WITH product_segment As(
	SELECT 
		product_key,
		product_name,
		cost,
		CASE 
			WHEN cost < 100 THEN "below 100"
			WHEN cost BETWEEN 100 AND 500 THEN "100-500"
			WHEN cost BETWEEN 500 AND 1000 THEN "500-1000"
		ELSE "Above 1000"
		END As cost_range
	FROM gold_dim_product)
SELECT 
	cost_range,
	count(product_name)AS product_count
FROM product_segment
GROUP BY cost_range
ORDER BY product_count DESC;

-- Group customers into three segments based on their spending behaviour
-- Vip: at least 12 months of history and spending more than #5000.
-- Regular: at least 12 months of history and spending of #5000 or less.
-- New: lifespan less than 12 months
-- And the total number of customers by each group
WITH customer_spending AS (
	SELECT
		c.customer_key,
		sum(f.sales) AS Total_spending,
		MIN(order_date) AS first_order,
		MAX(order_date) AS Last_order,
		timestampdiff(Month, MIN(order_date), MAX(order_date)) AS Lifespan
	FROM  gold_fact_sales f
	LEFT JOIN gold_dim_customer c
		ON c.customer_key = f.customer_key
	GROUP BY c.customer_key
    )
SELECT
customer_segment,
count(customer_key) AS Total_customers
FROM
	(SELECT 
		customer_key,
		CASE 
			WHEN Lifespan >= 12 AND Total_spending > 5000 THEN "VIP"
			WHEN Lifespan >= 12 AND Total_spending <= 5000 THEN "Regular"
		ELSE "New"
		END customer_segment
	FROM customer_spending)t
    GROUP BY customer_segment
    ORDER BY Total_customers DESC;
    

























