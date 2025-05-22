/* The code builds the **gold layer** of a data warehouse by transforming and integrating CRM and ERP data into refined, analysis-ready 
views for customers, products, and sales; it includes gender reconciliation, filters out inactive products, assigns surrogate keys, and 
validates referential integrity across fact and dimension tables to support accurate business intelligence reporting.*/

SELECT 
	Distinct sc.cst_gndr As Gender, 
	se.GEN As Gender,
	CASE 
		WHEN sc.cst_gndr != "n/a" THEN sc.cst_gndr
		ELSE coalesce(se.GEN, "n/a")
        END Gender
FROM silver_crm_cust_info sc
LEFT JOIN silver_erp_cust_az12 se
ON sc.cst_key = se.CID
LEFT JOIN silver_erp_loc_a101 sel
ON sel.CID = sc.cst_key
ORDER BY 1,2;

-- Virtual table for the Customer dimention table
-- Checking for duplicate
-- SELECT 
-- 	cst_key,
-- 	COUNT(*)
-- FROM
CREATE VIEW gold_dim_customer AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY cst_key) AS customer_Key,
		sc.cst_id AS customer_id, 
		sc.cst_key AS customer_number, 
		sc.cst_firstname AS First_name, 
		sc.cst_lastname AS Last_name, 
        sel.CNTRY As country,
		sc.cst_marital_status As Marital_status, 
        CASE 
			WHEN sc.cst_gndr != "n/a" THEN sc.cst_gndr
		ELSE coalesce(se.GEN, "n/a")
        END Gender,
        se.BDATE AS Birth_date,
		sc.cst_create_date AS create_date
	FROM silver_crm_cust_info sc
	LEFT JOIN silver_erp_cust_az12 se
		ON sc.cst_key = se.CID
    LEFT JOIN silver_erp_loc_a101 sel
		ON sel.CID = sc.cst_key;
    -- ) t
-- GROUP BY cst_key
-- HAVING COUNT(*) > 1
SELECT * FROM data_warehouse.gold_dim_customer;

-- Virtual table for the product dimention table
-- SELECT 
-- 	count(*),
-- 	prd_key
-- FROM(
CREATE VIEW gold_dim_product AS 
	SELECT 
		ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) AS product_key,
		scp.prd_id AS Product_id ,
        scp.prd_key As Product_number,
        scp.prd_nm AS Product_name,
		scp.Cat_id As Category_id,
        sep.CAT AS Category, 
		sep.SUBCAT AS Sub_category,
        sep.MAINTENANCE AS Maintenance,
		scp.prd_cost AS Cost, 
		scp.prd_line AS Product_line, 
		scp.prd_start_dt As start_date
	FROM silver_crm_prd_info scp
	LEFT JOIN silver_erp_px_cat_g1v2 sep
		ON sep.ID = scp.Cat_id
	WHERE scp.prd_end_dt IS NULL; -- Filter out all historical data
-- ) t 
-- GROUP BY prd_key
-- HAVING COUNT(*) > 1;
SELECT * FROM gold_dim_product;

-- Virtual table for the sales fact table
CREATE VIEW gold_fact_sales AS
SELECT 
	sc.sls_ord_num AS Order_number, 
    gdp.product_key,
    gdc.customer_Key,
	sc.sls_order_dt AS Order_date, 
	sc.sls_ship_dt AS Shipping_date, 
	sc.sls_due_dt AS Due_date, 
	sc.sls_sales AS Sales, 
	sc.sls_quantity As Quantity, 
	sc.sls_price AS Price
FROM silver_crm_sales_details sc
LEFT JOIN gold_dim_product gdp
	ON sc.sls_prd_key = gdp.product_number
LEFT JOIN gold_dim_customer gdc
	ON sc.sls_cust_id = gdc.customer_id;
    
SELECT * 
FROM gold_fact_sales;

-- Foreign key integrity check
SELECT * 
FROM gold_fact_sales f
LEFT JOIN gold_dim_customer c
	ON f.customer_key = c.customer_key
LEFT JOIN gold_dim_product p
	ON p.product_key = f.product_key
WHERE c.customer_key IS NULL;

SELECT * 
FROM gold_fact_sales f
LEFT JOIN gold_dim_customer c
	ON f.customer_key = c.customer_key
LEFT JOIN gold_dim_product p
	ON p.product_key = f.product_key
WHERE p.product_key IS NULL






