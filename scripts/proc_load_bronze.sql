-- ==================================================================================================================================================
-- Load the data if you want to load locally in mysql
-- loading the file locally
SET GLOBAL local_infile = 0;
SHOW VARIABLES LIKE 'local_infile';
-- ===================================================================================================================================================
-- Load the data globall
-- ===================================================================================================================================================
Select @@secure_file_priv;

-- loading the customer information table
LOAD DATA INFILE 'C:/ProgramData/MySQL/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
SET cst_id = IF(@cst_id = '' OR @cst_id IS NULL, 0, @cst_id),  
    cst_key = IF(@cst_key = '' OR @cst_key IS NULL, 'No Key', @cst_key),  
    cst_firstname = IF(@cst_firstname = '' OR @cst_firstname IS NULL, 'Unknown Firstname', @cst_firstname),  
    cst_lastname = IF(@cst_lastname = '' OR @cst_lastname IS NULL, 'Unknown Lastname', @cst_lastname),  
    cst_marital_status = IF(@cst_marital_status = '' OR @cst_marital_status IS NULL, 'Unknown', @cst_marital_status),  
    cst_gndr = IF(@cst_gndr = '' OR @cst_gndr IS NULL, 'Not Specified', @cst_gndr),  
    cst_create_date = IF(@cst_create_date = '' OR @cst_create_date IS NULL OR TRIM(@cst_create_date) = '', 'Unknown Date', @cst_create_date);

-- loading the production information table
LOAD DATA INFILE 'C:/ProgramData/MySQL/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_dt, @prd_end_dt)
SET prd_id = IF(@prd_id = '' OR @prd_id IS NULL, 0, @prd_id),  
    prd_key = IF(@prd_key = '' OR @prd_key IS NULL, 'No Key', @prd_key),  
    prd_nm = IF(@prd_nm = '' OR @prd_nm IS NULL, 'Unknown Product Name', @prd_nm),  
    prd_cost = IF(@prd_cost = '' OR @prd_cost IS NULL, 0.00, @prd_cost),  
    prd_line = IF(@prd_line = '' OR @prd_line IS NULL, 'Unknown', @prd_line),  
    prd_start_dt = IF(@prd_start_dt = '' OR @prd_start_dt IS NULL, 'Not Specified', @prd_start_dt),  
    prd_end_dt = IF(@prd_end_dt = '' OR @prd_end_dt IS NULL OR TRIM(@prd_end_dt) = '', 'Unknown Date', @prd_end_dt);


-- loading the Sales details table
LOAD DATA INFILE 'C:/ProgramData/MySQL/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@sls_ord_num, @sls_prd_key, @sls_cust_id,	@sls_order_dt,	@sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
SET sls_ord_num = IF(@sls_ord_num = '' OR @sls_ord_num IS NULL, 0, @sls_ord_num),  
    sls_prd_key = IF(@sls_prd_ke = '' OR @sls_prd_ke IS NULL, 'No Key', @sls_prd_ke),  
    sls_cust_id = IF(@sls_cust_id = '' OR @sls_cust_id IS NULL, 'Unknown Product Name', @sls_cust_id),  
    sls_order_dt = IF(@sls_order_dt = '' OR @sls_order_dt IS NULL, 0.00, @sls_order_dt),  
    sls_ship_dt = IF(@sls_ship_dt = '' OR @sls_ship_dt IS NULL, 'Unknown', @sls_ship_dt),  
    sls_due_dt = IF(@sls_due_dt = '' OR @sls_due_dt IS NULL, 'Not Specified', @sls_due_dt),  
    sls_sales = IF(@sls_sales = '' OR @sls_sales IS NULL, 'Unknown', @sls_sales),
	sls_quantity = IF(@sls_quantity = '' OR @sls_quantity IS NULL, 'Not Specified', @sls_quantity),
	sls_price = IF(@sls_price = '' OR @sls_price IS NULL, 'Not Specified', @sls_price);

LOAD DATA INFILE 'C:/ProgramData/MySQL/CUST_AZ12.csv'
INTO TABLE erp_CUST_AZ12
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@CID, @BDATE, @GEN)
SET CID = IF(@CID = '' OR @CID IS NULL, 0, @CID),  
	BDATE = IF(@BDATE = '' OR @BDATE IS NULL, '1900-01-01', STR_TO_DATE(@BDATE, '%m/%d/%Y')), 
    GEN = IF(@GEN = '' OR@GENL, 'Unknown Product Name', @GEN);
    
LOAD DATA INFILE 'C:/ProgramData/MySQL/LOC_A101.csv'
INTO TABLE erp_LOC_A101
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@CID, @CNTRY)
SET CID = IF(@CID = '' OR @CID IS NULL, 0, @CID),  
    CNTRY= IF(@CNTRY = '' OR @CNTRY IS NULL, 'Unknown Product Name', @CNTRY);
    
LOAD DATA INFILE 'C:/ProgramData/MySQL/PX_CAT_G1V2.csv'
INTO TABLE erp_PX_CAT_G1V2
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@ID, @CAT,	@SUBCAT, @MAINTENANCE)
SET ID	= IF(@ID = '' OR @ID IS NULL, 0, @ID),
	CAT = IF(@CAT = '' OR @CAT IS NULL, 'Unknown Product Name', @CAT),
    SUBCAT = IF(@SUBCAT  = '' OR @SUBCAT  IS NULL, 'Unknown Product Name', @SUBCAT),	
    MAINTENANCE = IF(@MAINTENANCE = '' OR @MAINTENANCE IS NULL, 'Unknown Product Name', @MAINTENANCE);
