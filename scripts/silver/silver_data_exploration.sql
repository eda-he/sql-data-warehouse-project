/*
==========================================================
Data Quality Checks for Bronze Layer Tables to Determine
Necessary Transformation for Silve Layer
==========================================================
Script Purpose:
	This script checks the data quality of bronze layer tables to address issues 
	through data cleaning for silver layer tables.

*/



USE DataWarehouse;

-- ============================================================
--		Data Quality Checks for bronze.crm_cust_info
-- ============================================================

SELECT TOP (1000) *
FROM bronze.crm_cust_info;

-- Checking primary keys are unique and there are no nulls
-- Finding: Among the duplicates, the row with the latest creation date has the most complete information
-- Solution: Keep only the latest row among primary keys with duplicates. Remove nulls.

SELECT cst_id,
	COUNT(*) COUNT_cst_id
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT a.*
FROM bronze.crm_cust_info a
JOIN(
	SELECT cst_id,
		COUNT(*) COUNT_cst_id
	FROM bronze.crm_cust_info
	GROUP BY cst_id
	HAVING COUNT(*) > 1
)b ON a.cst_id = b.cst_id
ORDER BY cst_id, cst_create_date;


-- Check for leading and trailing white spaces in VARCHAR variables
-- Finding: There are 26 rows with trailing/leading spaces in the name fields
-- Solution: Apply TRIM to these variables
SELECT cst_firstname,
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname!= TRIM(cst_firstname)
	OR cst_lastname != TRIM(cst_lastname);


SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr!= TRIM(cst_gndr);


-- Check for consistency and standardized options for low cardinality variables
-- Finding: Values are abbreviated. There are nulls.
-- Solution: Replace values with full label or 'n/a' for nulls.
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;



-- ============================================================
--		Data Quality Checks for bronze.crm_prd_info
-- ============================================================
SELECT TOP(1000) *
FROM bronze.crm_prd_info;

-- Checking primary key is unique and not null
-- FINDING: No duplicates found; No nulls found.
SELECT prd_id,
	COUNT(prd_id) count_prd_id
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(prd_id) > 1 OR prd_id IS NULL;


-- Checking data integration needs
-- FINDING: To join bronze.crm_prd_info and bronze.crm_sales_details, will need to standardize prd_key in bronze.crm_prd_info
-- SOLUTION: If prd_key has 2 hyphens or less the entire value should be prd_key.
--			If prd-key has more than 2 hyphens, separate the first 5 characters and replace hyphen with underscore into cat_id 
--			and remainign characters into prd_key
SELECT DISTINCT prd_key
FROM bronze.crm_prd_info
-- WHERE prd_key LIKE '%BK-R93R-62%'
-- WHERE prd_key LIKE '%BC-%'
ORDER BY prd_key;


SELECT DISTINCT sls_prd_key
FROM bronze.crm_sales_details
-- WHERE sls_prd_key LIKE '%BK-R93R-62%'
-- WHERE sls_prd_key LIKE '%BC-%'
ORDER BY sls_prd_key;


SELECT DISTINCT ID
FROM bronze.erp_px_cat_g1v2
-- WHERE ID LIKE 'BI%'
ORDER BY ID;


SELECT prd_key,
	CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN prd_key
	ELSE SUBSTRING(prd_key,7,LEN(prd_key))
	END prd_key_new,
	CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN 'n/a'
	ELSE REPLACE(SUBSTRING(prd_key,1, 5), '-', '_')
	END cat_ID
FROM bronze.crm_prd_info;


-- Checking the matches between bronze.crm_prd_info and bronze.erp_px_cat_g1v2
-- FINDING: 590 cat_ID w/o matches with ID; 1 ID w/o match in cat_ID
SELECT DISTINCT * 
FROM (
	SELECT prd_key,
		CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN prd_key
		ELSE SUBSTRING(prd_key,7,LEN(prd_key))
		END prd_key_new,
		CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN 'n/a'
		ELSE REPLACE(SUBSTRING(prd_key,1, 5), '-', '_')
		END cat_ID
	FROM bronze.crm_prd_info
) t
WHERE cat_ID NOT IN
	(SELECT DISTINCT ID FROM bronze.erp_px_cat_g1v2);


SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE ID NOT IN (
	SELECT DISTINCT 
		CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN 'n/a'
		ELSE REPLACE(SUBSTRING(prd_key,1, 5), '-', '_')
		END cat_ID
	FROM bronze.crm_prd_info
);


-- Checking the matches between bronze.crm_prd_info and bronze.crm_sales_details
-- FINDING: 330 cat_ID w/o matches with ID; All sls_prd_key matched
SELECT DISTINCT * 
FROM (
	SELECT prd_key,
		CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN prd_key
		ELSE SUBSTRING(prd_key,7,LEN(prd_key))
		END prd_key_new
	FROM bronze.crm_prd_info
) t
WHERE prd_key_new NOT IN
	(SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details);


SELECT * 
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
	SELECT DISTINCT 
		CASE WHEN LEN(prd_key) - LEN(REPLACE(prd_key, '-', '')) <= 2 THEN prd_key
		ELSE SUBSTRING(prd_key,7,LEN(prd_key))
		END prd_key_new
	FROM bronze.crm_prd_info
);



-- Checking if string columns have leading/trailing spaces
-- FINDING: No leading/trailing spaces found.
SELECT prd_nm, 
	prd_line
FROM bronze.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)
	OR prd_line != TRIM(prd_line);


-- Checking if costs are negative, 0, or NULL
-- Finding: No no or negative costs found; NULLS found
-- SOLUTION: repalce nulls with 0
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;


SELECT 
	ISNULL(prd_cost, 0) prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <= 0 OR prd_cost IS NULL;


-- Spelling out the value for low cardinality variables
SELECT
	CASE WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
		WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
		WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Other Sales'
		WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line
FROM bronze.crm_prd_info;


-- Checking that end dates are after start dates
-- FINDING: 400 rows have end date before start date
-- SOLUTION: Overwrite these end dates with the next start date of the product - 1 day
SELECT *
FROM (
	SELECT prd_start_dt,
		CAST(CASE WHEN prd_end_dt < prd_start_dt THEN 
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))
			ELSE prd_end_dt 
			END AS DATE) prd_end_dt
	FROM bronze.crm_prd_info
) t
WHERE prd_end_dt < prd_start_dt;


-- Changing nulls to 'n/a' for string variables
SELECT 
	COALESCE(prd_nm, 'n/a') prd_nm
FROM bronze.crm_prd_info;



-- ============================================================
--		Data Quality Checks for bronze.crm_sales_details
-- ============================================================
SELECT TOP(1000) *
FROM bronze.crm_sales_details;

-- Checking primary keys are unique. sls_ord_num with sls_prd_key create unique IDs
-- FINDING: No duplicates found. No nulls found.
SELECT sls_ord_num,
	sls_prd_key,
	COUNT(*) count_ord_prd
FROM bronze.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key
HAVING COUNT(sls_ord_num) > 1
	OR sls_ord_num IS NULL
	OR sls_prd_key IS NULL;


-- Checking sls_cust_id matches bronze.crm_cust_info's cst_id
-- FINDING: All sls_cust_id matches with bronze.crm_cust_info's cst_id
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN 
	(SELECT DISTINCT cst_id
	FROM bronze.crm_cust_info);


-- Format the dates
-- FINDING: Most dates are formatted YYYYMMDD but 19 rows have indecipherable sls_order_dt
-- SOLUTION: Change the 19 rows into NULLs and format the rest as dates
SELECT 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) != 8 OR
	LEN(sls_ship_dt) != 8 OR
	LEN(sls_due_dt) != 8;


SELECT 
	sls_order_dt,
	CASE WHEN LEN(sls_order_dt) = 8 THEN CAST(TRIM(CAST(sls_order_dt AS VARCHAR(8))) AS DATE) 
	ELSE NULL
	END new_sls_order_dt,
	sls_ship_dt,
	CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) new_sls_ship_dt,
	sls_due_dt,
	CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) new_sls_due_dt
FROM bronze.crm_sales_details;


-- Checking ship date is after order date
-- FINDING: All ship dates are after the order date
SELECT new_sls_order_dt, new_sls_ship_dt
FROM (
	SELECT 
		sls_order_dt,
		CASE WHEN LEN(sls_order_dt) = 8 THEN CAST(TRIM(CAST(sls_order_dt AS VARCHAR(8))) AS DATE) 
		ELSE NULL
		END new_sls_order_dt,
		sls_ship_dt,
		CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) new_sls_ship_dt,
		sls_due_dt,
		CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) new_sls_due_dt
	FROM bronze.crm_sales_details
) t
WHERE new_sls_order_dt > new_sls_ship_dt;


-- Checking due date is after ship date
-- FINDING: All due dates are after the ship date
SELECT new_sls_due_dt, new_sls_order_dt
FROM (
	SELECT 
		sls_order_dt,
		CASE WHEN LEN(sls_order_dt) = 8 THEN CAST(TRIM(CAST(sls_order_dt AS VARCHAR(8))) AS DATE) 
		ELSE NULL
		END new_sls_order_dt,
		sls_ship_dt,
		CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) new_sls_ship_dt,
		sls_due_dt,
		CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) new_sls_due_dt
	FROM bronze.crm_sales_details
) t
WHERE new_sls_due_dt < new_sls_order_dt;


-- Checking sales, qunatity and price are not negative and not null
-- FINDING: There are 22 rows where one of the above columns' value is negative or null. 
--	Most rows have sls_sales = sls_price. All sls_quantity values are valise
-- SOLUTION: replace sls_sales with sls_price * sls_quantity and vice versa, whichever has a a valid value
SELECT sales_vs_price,
	COUNT(*) count_sales_vs_price
FROM (
	SELECT
		CASE WHEN sls_sales = sls_price THEN 'Same'
		ELSE 'Different'
		END sales_vs_price
	FROM bronze.crm_sales_details
) t
GROUP BY sales_vs_price;


SELECT *
FROM(
	SELECT
		CASE WHEN (sls_sales < 0 OR sls_sales IS NULL OR sls_sales / sls_quantity != sls_price) AND sls_price >= 0 THEN sls_price * sls_quantity
		ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE WHEN (sls_price < 0 OR sls_price IS NULL) AND sls_sales >= 0 THEN sls_sales / sls_quantity
		ELSE sls_price
		END sls_price
	FROM bronze.crm_sales_details
) t
WHERE sls_sales < 0 OR sls_sales IS NULL OR
	sls_price < 0 OR sls_price IS NULL OR
	sls_sales / sls_quantity != sls_price;


-- Check for nulls in sls_cust_id
-- FINDING: No nulls found.
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL;



-- ============================================================
--		Data Quality Checks for bronze.erp_cust_az12
-- ============================================================
SELECT TOP (1000) *
FROM bronze.erp_cust_az12;

-- Checking primary key is unique and not null
-- FINDING: Primary keys are unique and there are no nulls.
SELECT CID,
	COUNT(*) count_cid
FROM bronze.erp_cust_az12
GROUP BY CID
HAVING COUNT(*) > 1 
	OR CID IS NULL;


-- Checking bronze.erp_cust_az12's CID matches with crm_cust_info's cst_id
-- FINDING: crm_cust_info's cst_key does not have the prefix 'NAS' that is in bronze.erp_cust_az12's CID 
-- SOLUTION: Remove the prefix 'NAS' from bronze.erp_cust_az12's CID 
SELECT DISTINCT CID
FROM bronze.erp_cust_az12;

SELECT DISTINCT cst_key
FROM silver.crm_cust_info;

SELECT CID,
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE CID
	END CID
FROM bronze.erp_cust_az12
WHERE CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE CID
	END NOT IN 
	(SELECT DISTINCT cst_key FROM silver.crm_cust_info);


-- Checking column values with low cardinality
-- FINDING: Multiple values for the same gender. Blank cells present. NULLs present.
-- SOLUTION: Spell out gender. Change NULL and blanks to n/a.
SELECT DISTINCT GEN
FROM bronze.erp_cust_az12;

SELECT DISTINCT *
FROM (
	SELECT 
		CASE TRIM(UPPER(GEN))
		WHEN 'F' THEN 'Female'
		WHEN 'FEMALE' THEN 'Female'
		WHEN 'M' THEN 'Male'
		WHEN 'MALE' THEN 'Male'
		ELSE 'n/a'
		END GEN
	FROM bronze.erp_cust_az12
)t;


-- Check birthdays are reasonable
-- FINDING: 16 birth dates are in the future
SELECT BDATE,
	CASE WHEN BDATE > GETDATE() THEN NULL
	ELSE BDATE
	END BDATE
FROM bronze.erp_cust_az12
WHERE BDATE < '1900-01-01' or BDATE > GETDATE();


-- ============================================================
--		Data Quality Checks for bronze.erp_loc_a101
-- ============================================================
SELECT TOP (1000) *
FROM bronze.erp_loc_a101;

-- Checking primary keys are unique and not NULL
-- FINDING: no duplicate or NULLs found.
SELECT CID,
	COUNT(*) count_cid
FROM bronze.erp_loc_a101
GROUP BY CID
HAVING COUNT(*) > 1
	OR CID IS NULL;

-- Checking the CID in erp_loc_a101 matches the cst_key in crm_cust_info
-- FINDING: erp_loc_a101's CID has an additional '-'
-- SOLUTION: Remove the '-' in erp_loc_a101's CID

SELECT CID
FROM bronze.erp_loc_a101;

SELECT cst_key
FROM silver.crm_cust_info;

SELECT 
	REPLACE(CID, '-', '') CID
FROM bronze.erp_loc_a101
WHERE REPLACE(CID, '-', '') NOT IN
	(SELECT DISTINCT cst_key
	FROM silver.crm_cust_info);


-- Checking column values with low cardinality
-- FINDING: Multiple values for the same country. Blank cells present. NULLs present.
-- SOLUTION: Spell out country. Change NULL and blanks to n/a.
SELECT DISTINCT CNTRY
FROM bronze.erp_loc_a101;


SELECT DISTINCT *
FROM (
	SELECT
		CASE WHEN TRIM(UPPER(CNTRY)) IN ('DE', 'GERMANY') THEN 'Germany'
			WHEN TRIM(UPPER(CNTRY)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
			WHEN TRIM(UPPER(CNTRY)) IN ('AU', 'AUSTRALIA') THEN 'Australia'
			WHEN TRIM(UPPER(CNTRY)) IN ('CA', 'CANADA') THEN 'Canada'
			WHEN TRIM(UPPER(CNTRY)) IN ('GB', 'UNITED KINGDOM') THEN 'United Kingdom'
			WHEN TRIM(UPPER(CNTRY)) IN ('FR', 'FRANCE') THEN ' France'
			WHEN TRIM(UPPER(CNTRY)) = '' OR CNTRY IS NULL THEN 'n/a'
			END CNTRY
	FROM bronze.erp_loc_a101
)t ;


-- ============================================================
--		Data Quality Checks for bronze.erp_px_cat_g1v2
-- ============================================================
SELECT TOP (1000) *
FROM bronze.erp_px_cat_g1v2;


-- Checking primary keys are unique and not NULL
-- FINDING: no duplicate or NULLs found.
SELECT ID,
	COUNT(ID)
FROM bronze.erp_px_cat_g1v2
GROUP BY ID
HAVING COUNT(ID) > 1 or ID IS NULL;


-- Checking erp_px_cat_g1v2's ID matches with crm_prd_cust_info's prd_key
-- FINDING: only one ID not matched

SELECT DISTINCT ID
FROM bronze.erp_px_cat_g1v2
WHERE ID NOT IN 
	(SELECT DISTINCT cat_id
	FROM silver.crm_prd_info);


-- Checking for leading/trailing spaces
-- FINDING: No leading/trailing spaces found.
SELECT
	CAT,
	SUBCAT,
	MAINTENANCE
FROM bronze.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT)
	OR SUBCAT != TRIM(SUBCAT)
	OR MAINTENANCE != TRIM(MAINTENANCE);


-- Checking column values with low cardinality
-- FINDING: Multiple values for the same country. Blank cells present. NULLs present.
-- SOLUTION: Spell out country. Change NULL and blanks to n/a.
SELECT DISTINCT
	MAINTENANCE
FROM bronze.erp_px_cat_g1v2;
