
/*
CREATE DATABASE AND SCHEMAS
Script Purpose : 
This create a new database named 'DATAWarehouse' after checking if it already exists.
Additionally ,the script sets up three schemas within the database : bronze,silver and gold.

WARNINGS:
Runing this script will drop the entire 'Datawarehouse' database if it exists.
All data in the database will permanently deleted.proceed with caution and ensure you have proper backups before running this script.

*/

USE master;
GO

-- drop and recreate the database 
IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- create the database warehouse--
create database DataWarehouse;
GO


USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
