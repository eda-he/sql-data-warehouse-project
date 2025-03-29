/*
==========================================================
Create Stored Procedure to Truncate and Insert Data Into 
Bronze Layer in 'DataWarehouse' Database
==========================================================
Script Purpose:
	This script creates a stored procedure ('bronze.load_bronze')that truncates and bulk uploads data 
	from csv files to the bronze layer tables. It perfroms the following actions for each table:
		- Truncate table, removing all data
		- Use 'BULK INSERT' to load data from csv file

WARNING:
	The script will delete any existing data within tables in the bronze layer and
	re-ingest the data from the csv files.
*/


-- USE DataWarehouse;

-- Creating stored procedure for loading bronze layer
-- CREATE PROCEDURE bronze.load_bronze AS
ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATE, @end_batch_time DATE;
	BEGIN TRY
		SET @start_batch_time = GETDATE();

		PRINT '=============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================';

		PRINT '.............................................';
		PRINT 'Loading CRM Tables';
		PRINT '.............................................';

		SET @start_time = GETDATE();

		-- Truncate and insert data into tables created in bronze_ddl.sql
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\edahe\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load Duration (bronze.crm_cust_info): ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';


		-- Checking data loaded correctly
		SELECT *
		FROM bronze.crm_cust_info;

		SELECT COUNT(*) AS crm_cust_info_COUNT
		FROM bronze.crm_cust_info;

		--
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\edahe\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load Duration (bronze.crm_prd_info): ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SELECT *
		FROM bronze.crm_prd_info;

		SELECT COUNT(*) AS crm_prd_info_COUNT
		FROM bronze.crm_prd_info;

		--
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\edahe\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();

		PRINT '>> Load Duration (crm_sales_details): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SELECT * 
		FROM bronze.crm_sales_details;

		SELECT COUNT(*) AS crm_sales_details_COUNT
		FROM bronze.crm_sales_details;


		PRINT '.............................................';
		PRINT 'Loading ERP Tables';
		PRINT '.............................................';
		--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\edahe\Documents\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();

		PRINT '>> Load Duration (erp_cust_az12): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SELECT * 
		FROM bronze.erp_cust_az12;

		SELECT COUNT(*) AS erp_cust_az12_COUNT
		FROM bronze.erp_cust_az12;

		--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\edahe\Documents\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();

		PRINT '>> Load Duration (erp_loc_a101): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SELECT * 
		FROM bronze.erp_loc_a101;

		SELECT COUNT(*) AS erp_loc_a101_COUNT
		FROM bronze.erp_loc_a101;

		--
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\edahe\Documents\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();

		PRINT '>> Load Duration (erp_px_cat_g1v2): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

		SELECT *
		FROM bronze.erp_px_cat_g1v2;

		SELECT COUNT(*) AS erp_px_cat_g1v2_COUNT
		FROM bronze.erp_px_cat_g1v2;

		SET @end_batch_time = GETDATE();
		PRINT '..........................................................................................';
		PRINT ' ';
		PRINT '>> Batch Load Duration: ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' seconds';
		PRINT ' ';
		PRINT '..........................................................................................';

	END TRY

	BEGIN CATCH
	PRINT '=============================================';
	PRINT '	EERROR OCCURED DURING BRONZE LAYER LOADING';
	PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR) + ': ' + ERROR_MESSAGE();
	PRINT '=============================================';
	END CATCH
END
