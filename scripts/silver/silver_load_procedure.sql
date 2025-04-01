/*
==========================================================
Create Stored Procedure to Truncate and Insert Data Into 
Silver Layer in 'DataWarehouse' Database
==========================================================
Script Purpose:
	This script creates a stored procedure ('silver.load_silver') that truncates and bulk uploads cleaned 
	data from the bronze layer tables into silver layer tables. It perfroms the following actions for each table:
		- Truncate table, removing all data
		- Use 'BULK INSERT' to load data from csv file

WARNING:
	The script will delete any existing data within tables in the bronze layer and
	re-ingest the data from the csv files.
*/


-- USE DataWarehouse;

-- CREATE PROCEDURE silver.load_silver AS
ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '=============================================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================================';

		PRINT '.............................................';
		PRINT 'Loading CRM Tables';
		PRINT '.............................................';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;  
		PRINT '>> Inserting Data Into Table: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr, 
			cst_create_date)

		-- Code to clean issues found in silver.crm_cust_info
		SELECT cst_id, 
			cst_key, 
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
				END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
				END cst_gndr,
			cst_create_date
		FROM (
			SELECT *, 
				RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_latest
			FROM silver.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_latest = 1;

		SET @end_time = GETDATE();

		PRINT '>> Load Duration (silver.crm_cust_info): ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;  
		PRINT '>> Inserting Data Into Table: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)

		-- Code to clean silver.crm_prd_info
		SELECT 
			prd_id,
				CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN 'n/a'
				ELSE REPLACE(SUBSTRING(prd_key,1, 5), '-', '_')
				END cat_id,
			CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN prd_key
				ELSE SUBSTRING(prd_key,7,LEN(prd_key))
				END prd_key,
			COALESCE(prd_nm, 'n/a') prd_nm,
			ISNULL(prd_cost, 0) prd_cost,
			CASE  TRIM(UPPER(prd_line)) 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line,
			CAST(prd_start_dt AS DATE),
			CAST(CASE WHEN prd_end_dt < prd_start_dt THEN 
					DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))
					ELSE prd_end_dt 
					END AS DATE) prd_end_dt
		FROM silver.crm_prd_info;

		SET @end_time = GETDATE();

		PRINT '>> Load Duration (silver.crm_prd_info): ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;  
		PRINT '>> Inserting Data Into Table: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)

		-- Code to clean crm_sales_details
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN LEN(sls_order_dt) = 8 THEN CAST(TRIM(CAST(sls_order_dt AS VARCHAR(8))) AS DATE) 
			ELSE NULL
			END sls_order_dt,
			CASE WHEN LEN(sls_ship_dt) = 8 THEN CAST(TRIM(CAST(sls_ship_dt AS VARCHAR(8))) AS DATE) 
			ELSE NULL
			END sls_ship_dt,
			CASE WHEN LEN(sls_due_dt) = 8 THEN CAST(TRIM(CAST(sls_due_dt AS VARCHAR(8))) AS DATE) 
			ELSE NULL
			END sls_due_dt,
			CASE WHEN (sls_sales < 0 OR sls_sales IS NULL OR sls_sales / sls_quantity != sls_price) AND sls_price >= 0 THEN sls_price * sls_quantity
			ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN (sls_price < 0 OR sls_price IS NULL) AND sls_sales >= 0 THEN sls_sales / sls_quantity
			ELSE sls_price
			END sls_price
		FROM silver.crm_sales_details;

		SET @end_time = GETDATE();

		PRINT '>> Load Duration (silver.crm_sales_details): ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;  
		PRINT '>> Inserting Data Into Table: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			CID,
			BDATE,
			GEN)

		-- Code to clean erp_cust_az12
		SELECT 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			ELSE CID
			END CID,
			CASE WHEN BDATE > GETDATE() THEN NULL
			ELSE BDATE
			END BDATE,
			CASE TRIM(UPPER(GEN))
				WHEN 'F' THEN 'Female'
				WHEN 'FEMALE' THEN 'Female'
				WHEN 'M' THEN 'Male'
				WHEN 'MALE' THEN 'Male'
				ELSE 'n/a'
				END GEN
		FROM silver.erp_cust_az12;

		SET @end_time = GETDATE();

		PRINT '>> Load Duration (silver.erp_cust_az12): ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;  
		PRINT '>> Inserting Data Into Table: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			CID,
			CNTRY
		)

		SELECT 
			REPLACE(CID, '-', '') CID,
			CASE WHEN TRIM(UPPER(CNTRY)) IN ('DE', 'GERMANY') THEN 'Germany'
					WHEN TRIM(UPPER(CNTRY)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
					WHEN TRIM(UPPER(CNTRY)) IN ('AU', 'AUSTRALIA') THEN 'Australia'
					WHEN TRIM(UPPER(CNTRY)) IN ('CA', 'CANADA') THEN 'Canada'
					WHEN TRIM(UPPER(CNTRY)) IN ('GB', 'UNITED KINGDOM') THEN 'United Kingdom'
					WHEN TRIM(UPPER(CNTRY)) IN ('FR', 'FRANCE') THEN ' France'
					WHEN TRIM(UPPER(CNTRY)) = '' OR CNTRY IS NULL THEN 'n/a'
					END CNTRY
		FROM silver.erp_loc_a101;



		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;  
		PRINT '>> Inserting Data Into Table: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			ID,
			CAT,
			SUBCAT,
			Maintenance
		)

		SELECT 
			ID,
			TRIM(CAT) CAT,
			TRIM(SUBCAT) SUBCAT,
			TRIM(Maintenance) Maintenance
		FROM silver.erp_px_cat_g1v2;

		SET @end_time = GETDATE();

		PRINT '>> Load Duration (silver.erp_loc_a101): ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SET @batch_end_time = GETDATE();
		PRINT '..........................................................................................';
		PRINT ' ';
		PRINT '>> Batch Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';
		

	END TRY

	BEGIN CATCH
	PRINT '=============================================';
	PRINT 'ERROR OCCURED DURING SILVER LAYER LOADING';
	PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR) + ': ' + ERROR_MESSAGE();
	PRINT '=============================================';
	END CATCH
END


EXECUTE silver.load_silver;
