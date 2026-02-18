/*
======================================================================================
Stored Procedure: Load Silver Layer (Bronze ->Silver)
======================================================================================
Script Purpose:
      This stored procedure performance the ETL (Extract, Transform, Load) process to 
      populate the 'silver' schema tables from the 'bronze' schema.
    Action Performed:
          - Truncates silver tables.
          - Insert transformed and cleansed data from Bronze into silver tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
 EXEC Silver.load_silver
===================================================================================
*/

create or alter procedure silver.load_silver as 
begin
		Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	Begin Try
		Set @batch_start_time = Getdate();

		Print '===================================================';
		Print 'Loading Silver Layer';
		Print '===================================================';

		Print '---------------------------------------------------';
		Print 'Loading CRM Tables';
		Print '---------------------------------------------------';

		-- Loading silver.crm_cust_info

		Set @start_time = getdate();

	print '>> Truncating Table: silver.crm_cust_info';
	Truncate Table silver.crm_cust_info;
	print '>> Inserting Data Into: silver.crm_cust_info';
	Insert Into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
select 
	cst_id,
	cst_key,
	Trim(cst_firstname) as cst_firstname,
	Trim(cst_lastname) as cst_lastname,
	case 
		When Upper(Trim(cst_marital_status)) =	'S' Then 'Single'
		When Upper(Trim(cst_marital_status)) =  'M' Then 'Married'
		else 'N/A'
		end as cst_marital_status,
    case
		When Upper(Trim(cst_gndr)) = 'F' Then 'Female'
		When Upper(Trim(cst_gndr)) = 'M' Then 'Male'
		else 'N/A'
		end as cst_gndr,
	cst_create_date
	from ( select *,
				ROW_NUMBER() over (Partition by cst_id order by cst_create_date desc) as flag_last
		  from bronze.crm_cust_info
		  where cst_id is not null 
		  )t
		  where flag_last = 1;
		  
	Set @end_time = GETDATE();

	Print '>> Loading Duration:' + cast(Datediff(second, @Start_time, @end_time)as varchar) + 'Seconds';
	Print '------------';
	

	-- Loading silver.crm_prd_Info

	Set @start_time = GETDATE();
	   Print '>> Truncating Table: Silver.crm_prd_info';
	   Truncate Table silver.crm_prd_info;
	   Print '>> Inserting Value Into: silver.crm_prd_info';
	   Insert Into silver.crm_prd_Info (
	   prd_id,
	   prd_key,
	   cat_id,
	   prd_nm,
	   prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt
	)

		select 
		prd_id,
		Substring(prd_nm, 7 , Len(prd_key)) as prd_key,
		Replace(Substring(Prd_key, 1 ,5), '-', '_') AS cat_id,
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		case
			When Upper(Trim(prd_line)) = 'M' Then 'Mountain'
			When Upper(Trim(prd_line)) = 'R' Then 'Road'
			When Upper(Trim(prd_line)) = 's' Then 'Other Sales'
			When Upper(Trim(prd_line)) = 'T' Then 'Touring'
			else 'N/A'
			end as prd_line,
		CAST(prd_start_dt as Date) as prd_dt,
		cast(
			Lead(prd_start_dt)  over (Partition by prd_key Order by prd_start_dt) -1
			as Date) as prd_end_dt
		From bronze.crm_prd_info;

		set @end_time = GETDATE();
		Print '>> Loading Duration:' + cast(Datediff(second, @Start_time, @end_time)as varchar) + 'Seconds';
		Print '------------';

		-- Loading  silver.crm_sales_details

		set @start_time = getdate();
	
		Print '>> Truncating Table: silver.crm_sales_details';
		Truncate Table silver.crm_sales_details;
		Print '>> Inserting Table: silver.crm_sales_details';
		Insert Into silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)

	(Select
	    sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt = '0' OR LEN(sls_order_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		CASE 
			When sls_sales != sls_quantity * abs(sls_price) or sls_sales <= 0  or sls_sales is null 
			then sls_quantity * abs(sls_price)
			else sls_sales
			end as sls_sales
			,
		sls_quantity,
		case 
			when sls_price <=0 or sls_price is null
			then sls_sales / nullif(sls_quantity,0)
			else sls_price
			end as sls_price
  FROM Bronze.crm_sales_details);
  
  set @end_time = getdate();
		Print '>> Loading Duration:' + cast(Datediff(second, @Start_time, @end_time)as varchar) + 'Seconds';
		Print '------------';


		Print '---------------------------------------------------';
		Print 'Loading ERP Tables';
		Print '---------------------------------------------------';

-- Loading Silver.erp_cust_az12
 
 Set @start_time = getdate();
 
  Print '>> Truncating Table:Silver.erp_cust_az12'
			Truncate Table silver.erp_cust_az12;
  Print '>> Inserting Table:silver.erp_cust_az12'
  insert into silver.erp_cust_az12(cid,bdate,gen)
  select  
  Case 
	   When cid like 'NAS%' then substring(cid, 4, len(cid)) 

	   else cid
	   end as cid,
	   case when bdate > getdate() then null
	   else bdate
	   end bdate,

  case When upper(trim(gen)) in ( 'M','male') then 'Male'
	   When upper(trim(gen)) in ('F','Female') Then 'Female'
	   else 'n/a'
	   end as gen
 
  from Bronze.erp_cust_az12;

  set @end_time = getdate();
  Print '>> Loading Duration:' + cast(Datediff(second, @Start_time, @end_time)as varchar) + 'Seconds';
  Print '------------';

  -- Loading Silver.erp_cat_g1v2
  
	Print '>> Truncating Table: Silver.erp_cat_g1v2'
	Truncate Table Silver.erp_cat_g1v2;
	Print '>> Inserting Table: Silver.erp_cat_g1v2'
	Insert Into Silver.erp_cat_g1v2 (id,cat,subcat,maintenance)

	select 
	id,
	cat,
	subcat,
	maintenance
	from Bronze.erp_cat_g1v2

	set @end_time = GETDATE();
	Print '>> Loading Duration:' + cast(Datediff(second, @Start_time, @end_time)as varchar) + 'Seconds';
	Print '------------';
	-- Loading silver.erp_loc_a101

	set @start_time = GETDATE();

	Print '>> Truncating table: silver.erp_loc_a101'
	Truncate Table Silver.erp_loc_a101;
	Print '>> Inserting Table:Silver.erp_loc_a101'
	Insert Into silver.erp_loc_a101(cid,cntry)
	select 
	replace (cid, '-' , '') as cid,
	case when trim(cntry)  = 'DE'  Then 'Germany'
		 When trim(cntry) in ('USA' , 'US' ) Then 'United States'
		 when trim(cntry) = ' ' or cntry is null Then 'N/A'
		 else trim(cntry) 
		 end as cntry

	from Bronze.erp_loc_a101;


	set @end_time = getdate();
  Print '>> Loading Duration:' + cast(Datediff(second, @Start_time, @end_time)as varchar) + 'Seconds';
  Print '------------';

  set @batch_end_time = GETDATE();
  print '===================================================';
  print 'Loading silver Layer is completed';
  print '  - Total loading duration ' + cast(datediff(second,@batch_start_time,@batch_end_time)as varchar) + ' seconds';
  print '===================================================';

end try


begin catch
	Print '===================================================';
	Print 'Error Occured during loading Silver layer';
	print 'Error Message' + Error_message();
	print 'Error Message' + cast (error_number() as nvarchar);
	print 'Error Message' + cast (error_state() as nvarchar);
end catch
end;


-- exec silver.load_silver;
