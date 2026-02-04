/*
====================================
Create Database and schema 
====================================

Script Purpose:
      This script crates a new database named 'DataWarehouse' after checking if it exists.
      If the database exists,it is dropped and recreated. Additional, the script sets up three schemas
      within the database: 'Bronze', silver', and 'gold.

WARNING:
      Running this script will drop the entrie 'Datawarehouse' database if it exists.
      All data in the database will be permanently deleted. Proceed with caution 
      and ensure you have proper backups before running this script.
*/

Use master;

go

-- Drop and recreate the 'Datawarehouse' database

if exists (select 1 from sys.databases where name = 'Datawarehouse')
begin
    alter database Datawarehouse set Single_user with rollback immediate;
drop Database Datawarehouse;
end;

go
-- Create the 'Datawarehouse' Database 
create database Datawarehouse;
go

use Datawarehouse;
go

-- Create schemas

create schema Bronze;

go

create schema silver;
go

create schema gold;
go


















  
