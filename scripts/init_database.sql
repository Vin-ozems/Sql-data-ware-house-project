/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

DROP DATABASE IF EXISTS data_warehouse;
CREATE DATABASE Data_warehouse; -- Create data_ ware house 

USE Data_warehouse; -- using data_ warehouse

-- =======================================================================================================================================================
-- Creating individual tables
-- =======================================================================================================================================================
-- Customer information table
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
	cst_id	INT,
	cst_key	VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),	
	cst_marital_status	VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

-- Product infomation table
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
	prd_id	INT,
	prd_key	VARCHAR(50),
	prd_nm	VARCHAR(50),
	prd_cost INT,	
	prd_line VARCHAR(50),	
	prd_start_dt DATE,	
	prd_end_dt DATE
);

-- Sales Details table
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
	sls_ord_num	VARCHAR(50),
	sls_prd_key	VARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt INT,	
	sls_ship_dt	INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,	
	sls_price INT
);

DROP TABLE IF EXISTS erp_CUST_AZ12;
CREATE TABLE erp_CUST_AZ12 (
    CID	VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50)
);

DROP TABLE IF EXISTS erp_LOC_A101;
CREATE TABLE erp_LOC_A101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS erp_PX_CAT_G1V2;
CREATE TABLE erp_PX_CAT_G1V2 (
    ID VARCHAR(50),
    CAT	VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);





