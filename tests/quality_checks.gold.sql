/*

Quality Check 
=================================================================
Script Purpose:
    This script perform quality checks to validate the intrgrity,consistency,
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purpose.

Usage Notes:
    - Run these checks after data loading silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
*/

--  ============================================================
--  Checking 'gold.dim_customers'
--  ============================================================
--  Check for uniqueness of customer key in gold.dim_customers
-- Expectation: No result
Select 
 customer_key,
 count(*) as duplicate_count
from gold.dim_customer
group by customer_key
having count(*) > 1;

--  ============================================================
--  Checking 'gold.product_key'
--  ============================================================
-- Check for uniqueness of product key in gold.dim_products
-- Expectation:No result

select
product_key,
count(*) as duplicate_count
from gold.dim_products
group by product_key
having count(*) > 1;

-- ============================================================
-- Checking 'gold.fact_sales'
-- ============================================================
-- Check the data model connectivity between fact and dimensions

select *
from gold.fact_sales f
left join gold.dim_customer t
on c.customer_key = t.customer_key
left join gold.dim_product p
on p.product_key = f.product_key
where p.product_key is null or customer_key is null;























