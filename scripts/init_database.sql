/*
==========================================================
Create Database and Schemas
==========================================================
Script Purpose:
	This script creates a new 'DataWarehouse' database. If a 'DataWarehouse' databases already exists,
	it will drop it to create a new one. The script also created three schemas: 'bronze', 'silver', and 'gold'.

WARNING:
	This script will drop the existing 'DataWarehouse' database, if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure proper backups are inplace before running the script.
*/


-- Create Database 'DataWarehouse'
USE master;
GO

-- Drop Databse if exists already
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse ;
END;
GO

-- Create 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO 

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

