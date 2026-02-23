/*
=================================================================================
DDL Script: Create Gold Views
=================================================================================
Script Purpose:
	This Script creates view for the gold layer in the data warehouse 
	The gold layer represents the final dimension and facts tables (Star Schema)

	Each view perform transformations and combines data from the silver layer
	to produce a clean, enriched, and business-ready dataset.

Usage:
	- These views can be queried directly for analytics and reporting.
==================================================================================
*/

print'======================================================';
print 'Create View Gold Dim of customers';
print'=====================================================';
go
	create or alter view Gold.dim_customer as
	SELECT
	ROW_NUMBER() OVER (order by ci.cst_id) as  customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as Marital_Status,

		case when ci.cst_gndr != 'n/a' then ci.cst_gndr  -- CRM is the master for gender Info
		else coalesce(ca.gen,'n/a')
		end as gender,

	ca.bdate as Birthday,
	ci.cst_create_date
	FROM silver.crm_cust_info ci
	left join silver.erp_cust_az12 as ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 as la
	on ci.cst_key = la.cid
	go
	
print'======================================================
-- Create View Gold Dim of Products'
print'======================================================'
go




	create or alter  view gold.dim_products as

	select 
	ROW_NUMBER() OVER (Order by ip.prd_start_dt,ip.prd_key) as Product_key,
	ip.prd_id as product_id,
	ip.prd_key as  Product_number,
	ip.prd_nm as  product_name,
	ip.cat_id as category_id,
	cr.cat as category,
	cr.subcat as subcategory,
	cr.maintenance,
	ip.prd_cost as cost,
	ip.prd_line as product_line,
	ip.prd_start_dt as start_date
	from silver.crm_prd_info as ip
	left join silver.erp_cat_g1v2 as cr
	on cr.id = ip.cat_id
	WHERE prd_end_dt is null -- Filter out all historical date;
	go
	
print'======================================================
-- Create View Gold facts of sales'
print'======================================================'
go


	create or alter view gold.fact_sales as
	select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_ampunt,
	sd.sls_quantity as quanity,
	sd.sls_price as price
	from silver.crm_sales_details as sd
	left join gold.dim_products as pr
	on sd.sls_prd_key = pr.product_number
	left join gold.dim_customer as cu
	on sd.sls_cust_id = cu.customer_id;









