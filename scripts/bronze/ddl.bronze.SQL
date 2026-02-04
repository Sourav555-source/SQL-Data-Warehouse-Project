/*
=========================================================================================
DDL Script: Create Bronze Tables
=========================================================================================
Script Purpose:
        This script creates tables un the 'bronze' schema, dropping existing tables
        if they already exist.
     Run this script to re-define the DDL structure of 'bronze' tables
========================================================================================
*/

use Datawarehouse;

if OBJECT_ID ('Bronze.crm_cust_info', 'u') IS NOT NULL
	Drop table Bronze.crm_cust_info;

create table Bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);


if OBJECT_ID ('Bronze.crm_prd_info', 'u') IS NOT NULL
	Drop table Bronze.crm_prd_info;

create table bronze.crm_prd_info (
prd_id int primary key,
prd_key nvarchar(50) not null,
prd_nm nvarchar(50),
prd_cost decimal(10,2),
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);


if OBJECT_ID ('Bronze.crm_sales_details', 'u') IS NOT NULL
	Drop table Bronze.crm_sales_details;


create table bronze.crm_sales_details (
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int
);


if OBJECT_ID ('Bronze.erp_loc_a101', 'u') IS NOT NULL
	Drop table Bronze.erp_loc_a101;


create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
);


if OBJECT_ID ('Bronze.erp_cust_az12', 'u') IS NOT NULL
	Drop table Bronze.erp_cust_az12;


create table bronze.erp_cust_az12 (
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);


if OBJECT_ID ('Bronze.erp_cat_g1v2', 'u') IS NOT NULL
	Drop table Bronze.erp_cat_g1v2;


create table bronze.erp_cat_g1v2(
id  nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);











