/*
==========================================================
Final Data Quality Checks for Silver Layer Tables After
the Laoding of Silver Tables
==========================================================
Script Purpose:
	This script checks the data quality of silver layer tables to 
	check the data has been cleaned properly.

*/

USE DataWarehouse;

-- ============================================================
--		FINAL Data Quality Checks for silver.crm_cust_info
-- ============================================================

-- Checking primary keys are unique and there are no nulls
-- Expectation: No results
SELECT cst_id,
	COUNT(*) COUNT_cst_id
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check for leading and trailing white spaces in VARCHAR variables
-- Expectation: No results
SELECT cst_firstname,
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname!= TRIM(cst_firstname)
	OR cst_lastname != TRIM(cst_lastname);


-- Check for consistency and standardized options for low cardinality variables
-- Expecation: No NULLs. Nulls replaces with n/a.
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;


SELECT *
FROM silver.crm_cust_info;


-- ============================================================
--		FINAL Data Quality Checks for silver.crm_prd_info
-- ============================================================

-- Checking primary key is unique and not null
-- Expectation: No duplicates found; No nulls found.
SELECT prd_id,
	COUNT(prd_id) count_prd_id
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(prd_id) > 1 OR prd_id IS NULL;


-- Checking if string columns have leading/trailing spaces
-- Expectation: Empty table.
SELECT prd_nm, 
	prd_line
FROM silver.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)
	OR prd_line != TRIM(prd_line);


-- Checking if costs are negative, 0, or NULL
-- Expectation: Empty table.
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- Checking: Labels are spelled out for columns with low cardinality
SELECT DISTINCT
	prd_line
FROM silver.crm_prd_info;


-- Checking that end dates are after start dates
-- Expectation:Empty table.
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt
	AND prd_end_dt IS NOT NULL;


-- Changing nulls to 'n/a' for string variables
-- Expectation: No nulls.
SELECT DISTINCT
	prd_nm
FROM silver.crm_prd_info;


SELECT *
FROM silver.crm_prd_info;


-- ============================================================
--		FINAL Data Quality Checks for silver.crm_sales_details
-- ============================================================

-- Checking primary keys are unique. sls_ord_num with sls_prd_key create unique IDs
-- EXPECTATON: Empty table.
SELECT sls_ord_num,
	sls_prd_key,
	COUNT(*) count_ord_prd
FROM silver.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key
HAVING COUNT(sls_ord_num) > 1
	OR sls_ord_num IS NULL
	OR sls_prd_key IS NULL;


-- Checking sls_cust_id matches silver.crm_cust_info's cst_id
-- EXPECTATON: Empty table.
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN 
	(SELECT DISTINCT cst_id
	FROM silver.crm_cust_info);


-- Checking sales, qunatity and price are not negative and not null
-- EXPECTATION: Empty table.
SELECT
	sls_sales,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales < 0 OR sls_sales IS NULL OR
	sls_price < 0 OR sls_price IS NULL OR
	sls_sales / sls_quantity != sls_price;


-- Check for nulls in sls_cust_id
-- EXPECATION: Empty table.
SELECT sls_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id IS NULL;


SELECT *
FROM silver.crm_sales_details;


-- ============================================================
--		FINAL Data Quality Checks for silver.crm_sales_details
-- ============================================================

-- Checking primary key is unique and not null
-- EXPECATION: Empty table.
SELECT CID,
	COUNT(*) count_cid
FROM silver.erp_cust_az12
GROUP BY CID
HAVING COUNT(*) > 1 
	OR CID IS NULL;


-- Checking column values with low cardinality
-- EXPECATION: Only value shsould be Female, Male, and n/a
SELECT DISTINCT GEN
FROM silver.erp_cust_az12;


SELECT *
FROM silver.erp_cust_az12;



-- ============================================================
--		FINAL Data Quality Checks for silver.erp_loc_a101
-- ============================================================

-- Checking primary key is unique and not null
-- EXPECATION: Empty table.
SELECT CID,
	COUNT(*) count_cid
FROM silver.erp_loc_a101
GROUP BY CID
HAVING COUNT(*) > 1
	OR CID IS NULL;


-- Checking the CID in erp_loc_a101 matches the cst_key in crm_cust_info
-- EXPECATION: Empty table.

SELECT 
	CID
FROM silver.erp_loc_a101
WHERE CID NOT IN
	(SELECT DISTINCT cst_key
	FROM silver.crm_cust_info);



-- ============================================================
--		FINAL Data Quality Checks for silver.px_cat_g1v2
-- ============================================================

-- Checking primary key is unique and not null
-- EXPECATION: Empty table.
SELECT ID,
	COUNT(ID)
FROM silver.erp_px_cat_g1v2
GROUP BY ID
HAVING COUNT(ID) > 1 or ID IS NULL;


-- Checking for leading/trailing spaces
-- EXPECATION: Empty table.
SELECT
	CAT,
	SUBCAT,
	MAINTENANCE
FROM silver.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT)
	OR SUBCAT != TRIM(SUBCAT)
	OR MAINTENANCE != TRIM(MAINTENANCE);
