/* Data definition for the silver layer: This SQL script defines the silver layer of a data warehouse, which organizes and standardizes 
cleaned data from CRM and ERP systems into structured tables for analytics and reporting. It includes the creation of six key tables across 
customer, product, and sales domains, with data enrichment fields for audit tracking. */

-- Customer information table
DROP TABLE IF EXISTS silver_crm_cust_info;
CREATE TABLE silver_crm_cust_info (
	cst_id	INT,
	cst_key	VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),	
	cst_marital_status	VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
    dwh_date_created DATETIME DEFAULT NOW()
);

-- Product infomation table
DROP TABLE IF EXISTS silver_crm_prd_info;
CREATE TABLE silver_crm_prd_info (
	prd_id INT,
    Cat_id VARCHAR(50),
	prd_key	VARCHAR(50),
	prd_nm	VARCHAR(50),
	prd_cost INT,	
	prd_line VARCHAR(50),	
	prd_start_dt DATE,	
	prd_end_dt DATE,
    dwh_date_created DATETIME DEFAULT NOW()
);

-- Sales Details table
DROP TABLE IF EXISTS silver_crm_sales_details;
CREATE TABLE silver_crm_sales_details (
	sls_ord_num	VARCHAR(50),
	sls_prd_key	VARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt DATE,	
	sls_ship_dt	DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,	
	sls_price INT,
    dwh_date_created DATETIME DEFAULT NOW()
);

DROP TABLE IF EXISTS silver_erp_CUST_AZ12;
CREATE TABLE silver_erp_CUST_AZ12 (
    CID	VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50),
    dwh_date_created DATETIME DEFAULT NOW()
);

DROP TABLE IF EXISTS silver_erp_LOC_A101;
CREATE TABLE silver_erp_LOC_A101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(50),
    dwh_date_created DATETIME DEFAULT NOW()
);

DROP TABLE IF EXISTS silver_erp_PX_CAT_G1V2;
CREATE TABLE silver_erp_PX_CAT_G1V2 (
    ID VARCHAR(50),
    CAT	VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50),
    dwh_date_created DATETIME DEFAULT NOW()
);

/* This SQL script performs data quality assessment, cleaning, standardization, normalization, and transformation across three key CRM data 
sources: crm_cust_info (customer information), crm_prd_info (product information), and crm_sales_details (sales transactions). The goal is to 
prepare clean, enriched, and consistent data for the silver layer of the data warehouse. And also to load the it into the silver layer*/
/*-----------------------------------------------------------------------------------------------------------------------------------------------------
Quality check for the crm_cust_info 
Data cleaning 
Data standardization
Data normalization
Derive columns
Data enrichment
--------------------------------------------------------------------------------------------------------------------------------------------------*/
-- Exploring and understanding the data from the bronze table 
SELECT * FROM crm_cust_info;

-- Checking for datatype correctness
DESC crm_cust_info;

-- Checking for null or duplicate in the customer information Table
-- Expectation no result a primary key must be unique and not null
-- identify duplicates
SELECT 
    COUNT(*)count, 
    cst_id
FROM crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 Or cst_id is null;

-- checking for duplicate rows
SELECT * 
FROM(SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
	 FROM crm_cust_info) t
WHERE flag_last != 1;

-- Checking for unwanted spaces 
-- No expectainon
SELECT cst_firstname
FROM crm_cust_info
WHERE cst_firstname != trim(cst_firstname);

SELECT cst_lastname
FROM crm_cust_info
WHERE cst_lastname != trim(cst_lastname);

SELECT cst_gndr
FROM crm_cust_info
WHERE cst_gndr != trim(cst_gndr);

SELECT cst_marital_status
FROM crm_cust_info
WHERE cst_marital_status != trim(cst_marital_status);

-- Data standardization and Consistency
SELECT count(DISTINCT cst_gndr)
FROM crm_cust_info;

SELECT count(DISTINCT cst_marital_status)
FROM crm_cust_info;

-- Ordering them to check for the most recent and also a compilation of the entire query for the cleaning and standardization
SELECT 
	cst_id, 
	cst_key, 
	TRIM(cst_firstname) cst_firstname, 
	TRIM(cst_lastname) cst_lastname, 
	cst_marital_status, 
	cst_gndr, 
    CASE 
		WHEN UPPER(TRIM(cst_gndr)) = "F" THEN "Female" -- Normalize marital status to a readable format
        WHEN UPPER(TRIM(cst_gndr)) = "M" THEN "Male"
	ELSE "N/A"
    END cst_gndr,
    CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = "S" THEN "Single" -- Normalize gender values to a readable format
        WHEN UPPER(TRIM(cst_marital_status)) = "M" THEN "Married"
	ELSE "N/A"
    END cst_marital_status,
	cst_create_date 
FROM (SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
	 FROM crm_cust_info) t
WHERE flag_last = 1; -- Retaining the most relevant rows

-- Inserting into the silver layer (silver_crm_cust_info)
INSERT INTO silver_crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id, 
    cst_key, 
    TRIM(cst_firstname), 
    TRIM(cst_lastname), 
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = "S" THEN "Single" -- Normalize marital status to a readable format
        WHEN UPPER(TRIM(cst_marital_status)) = "M" THEN "Married"
        ELSE "N/A"
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = "F" THEN "Female" -- Normalize gender values to a readable format
        WHEN UPPER(TRIM(cst_gndr)) = "M" THEN "Male"
        ELSE "N/A"
    END,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM crm_cust_info
) t
WHERE flag_last = 1; -- Retaining the most relevant rows

-- Exploring and understanding the data from the silver table
SELECT * FROM data_warehouse.silver_crm_cust_info;

/*-----------------------------------------------------------------------------------------------------------------------------------------------------
Quality check for the crm_prd_info 
Data cleaning 
Data standardization
Data normalization
Derive columns
Data enrichment
-----------------------------------------------------------------------------------------------------------------------------------------------------*/
-- Exploring and understanding the data from the bronze table 
SELECT * FROM crm_prd_info;

-- Checking for datatype correctness
DESC crm_prd_info;

-- Checking for null or duplicate in the customer product information 
-- Expectation no result
-- identify duplicates
SELECT 
    COUNT(*), 
    prd_id
FROM crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 Or prd_id is null;

-- Checking for unwanted spaces
SELECT prd_nm
FROM crm_prd_info
WHERE prd_nm != trim(prd_nm);

-- Checking for Null or Negative value
-- Expectations: No results
SELECT * 
FROM crm_prd_info
WHERE prd_cost < 0 OR prd_cost = 0;

-- Data standadization and consistency
SELECT DISTINCT prd_line
FROM crm_prd_info;

-- Checking for invalid date orders
SELECT * 
FROM  crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT 
	prd_id, 
	prd_key, 
	prd_nm,  
	prd_start_dt, 
	prd_end_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_start_dt_test
FROM crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- Ordering them to check for the most recent and also a compilation of the entire query for the cleaning and standardization
SELECT 
  prd_id, 
  REPLACE(SUBSTRING(prd_key, 1, 5), "-", "_") AS cat_id , -- Extract category id
  SUBSTRING(prd_key, 7) AS prd_Key, -- Extract product key
  prd_nm,
  IFNULL(prd_cost, 0) AS prd_cost,
  CASE UPPER(TRIM(prd_line))
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'M' THEN 'Mountain'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line,
  CAST(prd_start_dt AS DATE) AS prd_start_dt, 
  CAST(DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS DATE) AS prd_end_dt -- calculate the end date as one date before
FROM crm_prd_info;

-- Inserting into the silver layer (silver_prd_cust_info)
INSERT INTO silver_crm_prd_info(
	prd_id, 
    cat_id,
	prd_key, 
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt, 
	prd_end_dt)
SELECT 
  prd_id, 
  REPLACE(SUBSTRING(prd_key, 1, 5), "-", "_") AS cat_id , -- Extract category id
  SUBSTRING(prd_key, 7) AS prd_Key, -- Extract product key
  prd_nm,
  IFNULL(prd_cost, 0) AS prd_cost,
  CASE UPPER(TRIM(prd_line))
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'M' THEN 'Mountain'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line,
  CAST(prd_start_dt AS DATE) AS prd_start_dt, 
  CAST(DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS DATE) AS prd_end_dt -- calculate the end date as one date before
FROM crm_prd_info;

/*-----------------------------------------------------------------------------------------------------------------------------------------------------
Quality check for the crm_sales_details
Data cleaning 
Data standardization
Data normalization
Derive columns
Data enrichment
-----------------------------------------------------------------------------------------------------------------------------------------------------*/
-- Exploring and understanding the data from the bronze table(crm_sales_details)
SELECT * FROM crm_sales_details;

-- Checking for datatype correctness
DESC crm_sales_details;
SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales, 
	sls_quantity, 
	sls_price
FROM crm_sales_details
WHERE sls_ord_num != trim(sls_ord_num) 
AND sls_prd_key NOT IN (SELECT prd_key FROM crm_prd_info)
AND sls_cust_id NOT IN (SELECT cst_id FROM crm_cust_info);

-- checking for invalid dates that are in integer format
SELECT 
	sls_order_dt
FROM crm_sales_details
WHERE sls_order_dt = 0;

SELECT 
	nullif(sls_order_dt, 0) sls_order_dt
FROM crm_sales_details
WHERE sls_order_dt <= 0  
OR length(sls_order_dt) != 8
OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

SELECT 
nullif(sls_ship_dt, 0) sls_ship_dt
FROM crm_sales_details
WHERE sls_ship_dt <= 0  
OR length(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

SELECT 
nullif(sls_due_dt, 0) sls_ship_dt
FROM crm_sales_details
WHERE sls_due_dt <= 0  
OR length(sls_due_dt) != 8
OR sls_due_dt > 20500101 OR sls_due_dt < 19000101;

SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
    CASE WHEN sls_order_dt  <= 0 OR length(sls_order_dt) != 8 THEN NULL
		ELSE CAST(sls_order_dt AS DATE)
    END sls_order_dt,
    CASE WHEN sls_ship_dt  <= 0 OR length(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(sls_ship_dt AS DATE)
    END sls_ship_dt,
    CASE WHEN sls_due_dt  <= 0 OR length(sls_due_dt) != 8 THEN NULL
		ELSE CAST(sls_due_dt AS DATE)
    END sls_due_dt
FROM crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Checking for data consistency Between Sales, Quantity and price
-- >> Sales = Quantity * Price
-- >> Values must not be null, negative or Zero
SELECT 
	sls_quantity,
    sls_sales,
    sls_price,
    ROUND(ABS(sls_sales) / NULLIF(sls_quantity, 0)) AS price,
    IFNULL(ROUND(ABS(sls_sales) / NULLIF((ABS(sls_sales) / NULLIF(sls_quantity, 0)), 0)), 0) AS quantity,
    IFNULL(ROUND((ABS(sls_sales) / NULLIF(sls_quantity, 0)) * 
                 (ABS(sls_sales) / NULLIF((ABS(sls_sales) / NULLIF(sls_quantity, 0)), 0))), 0) AS Sales
FROM crm_sales_details
WHERE sls_sales!= sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price  <=0 
ORDER BY sls_sales, sls_quantity, sls_price;

-- Inserting into the silver layer (silver_crm_sales_details)
INSERT INTO silver_crm_sales_details(
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales, 
	sls_quantity, 
	sls_price)
SELECT 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
    CASE WHEN sls_order_dt  <= 0 OR length(sls_order_dt) != 8 THEN NULL
		ELSE CAST(sls_order_dt AS DATE)
    END sls_order_dt,
    CASE WHEN sls_ship_dt  <= 0 OR length(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(sls_ship_dt AS DATE)
    END sls_ship_dt,
    CASE WHEN sls_due_dt  <= 0 OR length(sls_due_dt) != 8 THEN NULL
		ELSE CAST(sls_due_dt AS DATE)
    END sls_due_dt,
    ROUND(ABS(sls_sales) / NULLIF(sls_quantity, 0)) AS price,
    IFNULL(ROUND(ABS(sls_sales) / NULLIF((ABS(sls_sales) / NULLIF(sls_quantity, 0)), 0)), 0) AS quantity,
    IFNULL(ROUND((ABS(sls_sales) / NULLIF(sls_quantity, 0)) * 
                 (ABS(sls_sales) / NULLIF((ABS(sls_sales) / NULLIF(sls_quantity, 0)), 0))), 0) AS Sales
FROM crm_sales_details;

/*-----------------------------------------------------------------------------------------------------------------------------------------------------
Quality check for the erp_cust_az12
Data cleaning 
Data standardization
Data normalization
Derive columns
Data enrichment
-----------------------------------------------------------------------------------------------------------------------------------------------------*/
-- Exploring and understanding the data from the bronze table(erp_cust_az12)
SELECT * FROM erp_cust_az12;

-- Checking for datatype correctness
DESC erp_cust_az12;

SELECT 
CID,
CASE 
	WHEN CID LIKE "%NAS%" THEN substring(CID, 4, length(CID)) 
ELSE CID
END CID_new
FROM erp_cust_az12;

-- identify out of range date
SELECT 
	DISTINCT BDATE
FROM erp_cust_az12
WHERE BDATE < "1924-01-01" OR BDATE > now();

SELECT 
	CASE 
		WHEN BDATE > now() THEN NULL
    ELSE BDATE
    END BDATE
FROM erp_cust_az12;

-- Data standardization and consistency
SELECT 
	DISTINCT GEN
FROM erp_cust_az12;

SELECT 
    DISTINCT GEN,
    CASE 
        WHEN UPPER(TRIM(GEN)) LIKE 'F%' THEN 'Female'
        WHEN UPPER(TRIM(GEN)) LIKE 'M%' THEN 'Male'
        ELSE 'n/a'
    END AS Standardized_GEN
FROM erp_cust_az12;

-- Inserting into the silver layer (silver_erp_cust_az12)
INSERT INTO silver_erp_cust_az12(
CID, 
BDATE, 
GEN
)
SELECT 
	CASE 
		WHEN CID LIKE "%NAS%" THEN substring(CID, 4, length(CID)) 
	ELSE CID
	END CID, # Remove "nas" prefix if present
	CASE 
		WHEN BDATE > now() THEN NULL
    ELSE BDATE
    END BDATE, # Set Future birhtdate to null
    CASE 
        WHEN UPPER(TRIM(GEN)) LIKE 'F%' THEN 'Female'
        WHEN UPPER(TRIM(GEN)) LIKE 'M%' THEN 'Male'
        ELSE 'n/a'
    END AS Standardized_GEN # Normalize gender value and handle unknown cases
FROM erp_cust_az12;

/*-----------------------------------------------------------------------------------------------------------------------------------------------------
Quality check for the crm_sales_details
Data cleaning 
Data standardization
Data normalization
Derive columns
Data enrichment
-----------------------------------------------------------------------------------------------------------------------------------------------------*/
-- Exploring and understanding the data from the bronze table(erp_cust_az12)
SELECT * FROM erp_loc_a101;

-- Checking for datatype correctness
DESC erp_loc_a101;

SELECT 
	CID, 
	CNTRY
FROM erp_loc_a101;

SELECT 
	replace(CID, "-", "") CID
FROM erp_loc_a101;

SELECT 
	DISTINCT CNTRY
FROM erp_loc_a101;

SELECT 
  DISTINCT CNTRY,
  CASE 
    WHEN REPLACE(REPLACE(UPPER(TRIM(CNTRY)), CHAR(13), ''), CHAR(10), '') = 'DE' THEN 'Germany'
    WHEN REPLACE(REPLACE(UPPER(TRIM(CNTRY)), CHAR(13), ''), CHAR(10), '') IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
    ELSE TRIM(CNTRY)
  END AS Standardized_CNTRY
FROM erp_loc_a101;

-- Inserting into the silver layer (silver_erp_loc_a101)
INSERT INTO silver_erp_loc_a101(
CID, 
CNTRY)
SELECT 
    replace(CID, "-", "") CID,
    CASE 
		WHEN REPLACE(REPLACE(UPPER(TRIM(CNTRY)), CHAR(13), ''), CHAR(10), '') = 'DE' THEN 'Germany'
		WHEN REPLACE(REPLACE(UPPER(TRIM(CNTRY)), CHAR(13), ''), CHAR(10), '') IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
		ELSE TRIM(CNTRY)
	END AS Standardized_CNTRY
FROM erp_loc_a101;

/*-----------------------------------------------------------------------------------------------------------------------------------------------------
Quality check for the crm_sales_details
-----------------------------------------------------------------------------------------------------------------------------------------------------*/
-- Exploring and understanding the data from the bronze table(erp_cust_az12)
SELECT * FROM erp_px_cat_g1v2;

-- Checking for datatype correctness
DESC erp_px_cat_g1v2;

SELECT 
	COUNT(*)
	ID
FROM erp_px_cat_g1v2
GROUP BY ID
HAVING COUNT(*) > 1;

-- Checking for unwanted spaces
SELECT 
	CAT
FROM erp_px_cat_g1v2
WHERE CAT != trim(CAT);

SELECT 
	DISTINCT CAT
FROM erp_px_cat_g1v2;

SELECT 
	SUBCAT
FROM erp_px_cat_g1v2
WHERE SUBCAT != Trim(SUBCAT);


SELECT 
  e.ID, 
  e.CAT, 
  e.SUBCAT, 
  e.MAINTENANCE
FROM erp_px_cat_g1v2 e
JOIN (
    SELECT 
      ID
    FROM erp_px_cat_g1v2
    GROUP BY ID
    HAVING COUNT(*) > 1
) t ON e.ID = t.ID
WHERE e.SUBCAT != TRIM(e.SUBCAT) OR e.CAT != TRIM(e.CAT);

-- Inserting into the silver layer(silver_erp_px_cat_g1v2)
INSERT INTO silver_erp_px_cat_g1v2(
	ID, 
    CAT, 
    SUBCAT, 
    MAINTENANCE)
SELECT 
	ID, 
    CAT, 
    SUBCAT, 
    MAINTENANCE
FROM erp_px_cat_g1v2













 



 

 
