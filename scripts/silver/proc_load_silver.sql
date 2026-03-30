/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time= GETDATE();
		PRINT '==============================================='
		PRINT 'Loading Silver Layer'
		PRINT '==============================================='

		PRINT '-----------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '-----------------------------------------------'
        --loading silver .crm_cust_info
		SET @start_time=GETDATE()
		Print '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT 'Inserting data into: silver.crm_cust_info'
		Insert Into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date) 
		Select cst_id,
		cst_key,
		TRIM(cst_firstname) As cst_firstname,
		TRIM(cst_lastname) As cst_lastname,
		case when UPPER(Trim(cst_marital_status))='S' then 'Single'
			 when UPPER(TRIM(cst_marital_status))='M' then 'Married'
			 else 'n/a'
		end as cst_marital_status,
		Case when Upper(Trim(cst_gndr))='F' then 'Female'
			 when Upper(Trim(cst_gndr))='M' then 'Male'
			 else 'n/a'
		end As cst_gndr,
		cst_create_date
		from (
		Select  
		*, ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last  
		from bronze.crm_cust_info
		where cst_id is not null) t
		where flag_last =1

		SET @end_time=GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT '>>------------'

		--loading crm_prd_info table
		SET @start_time=GETDATE();
		Print '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'Inserting data into: silver.crm_prd_info'
		Insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
		Select 
		prd_id,
		Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- Extract Category Id
		SUBSTRING(prd_key,7,Len(prd_key))  as prd_key, -- Extract prd_key
		prd_nm,
		isnull(prd_cost,0) as prd_cost,-- if product id is null, replace by 0 
		case Upper(Trim(prd_line))
			 when 'R' then 'Road'
			 when 'M' then 'Mountain'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		end as prd_line, -- Mapping product line codes to descriptive values
		Cast(prd_start_dt As Date) as prd_start_dt ,
		Cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt asc)-1 As Date)	
			as prd_end_dt -- Calculate end date as one day before the next start date
		from bronze.crm_prd_info ;
		SET @end_time=GETDATE();
		PRINT'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT '>>------------'

		--loading crm_sales_details
		SET  @start_time=GETDATE();
		Print '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'Inserting data into: silver.crm_sales_details'
		Insert into silver.crm_sales_details(
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

		Select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt <=0 or len(sls_order_dt) !=8 then Null
			 else cast(cast(sls_order_dt As varchar )as date) 
		end as sales_order_dt,
		case when sls_ship_dt <=0 or len(sls_ship_dt)!=8 then Null
			 else cast(cast(sls_ship_dt as varchar)as date) 
		end as sales_ship_dt,
		case when sls_due_dt <=0 or len(sls_due_dt) !=8 then Null 
			 else cast(cast(sls_due_dt as varchar)as date) 
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0  or sls_sales!= sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			 else sls_sales 
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0 
				then sls_sales / nullif(sls_quantity,0)
			else sls_price
		end as sls_price
		 from 
		bronze.crm_sales_details;

		SET @end_time=GETDATE();
		PRINT 'Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '>>------------'

		--loading erp_cust_az12 table
		SET @start_time= GETDATE();
		Print '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT 'Inserting data into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		select 
		case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
			 else cid

		end cid,
		case when bdate > GETDATE() then null  
			 else bdate
		end as bdate,
		case when UPPER(TRIM(gen)) in ('F','Female') then 'Female'
			 when UPPER(TRIM(gen)) in ('M','Male') then 'Male' 
			else 'n/a' 
		end as gen
		from
		bronze.erp_cust_az12;

		SET @end_time=GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) +'seconds'
		PRINT '>>------------'

		--loading erp_loc_a101 table

		SET @start_time=GETDATE();
		Print '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT 'Inserting data into: silver.erp_loc_a101'
		Insert into silver.erp_loc_a101 (cid,cntry)

		Select 
		REPLACE(cid,'-','') as cid,
		case when TRIM(cntry) ='DE' then 'Germany'
			 when TRIM(cntry) in ('US', 'USA') then 'United States'
			 when TRIM(cntry) ='' or cntry is null then 'n/a'
			 else cntry 
		end as cntry
	 
		from bronze.erp_loc_a101;

		SET @end_time=GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) +'seconds'
		PRINT '>>------------'

		--loading erp_px_cat_g1v2 table 
		SET @start_time=GETDATE();
		Print '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT 'Inserting data into: silver.erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		select 
		id,
		cat,
		subcat,
		maintenance

		from
		bronze.erp_px_cat_g1v2 ;
		SET @end_time=GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) +'seconds'
		PRINT '>>------------'

		SET @batch_end_time=GETDATE();
		PRINT '===================================='
		PRINT 'Loading Silver layer is completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(Second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds'
	END TRY
	BEGIN CATCH
		PRINT'=========================================';
		PRINT 'Error Occured during loading silver layer';
		PRINT 'Error Message' + Error_Message();
		PRINT 'Error Message' + CAST(Error_Number() AS NVARCHAR);
		PRINT 'Error Message' + CAST(Error_State() AS NVARCHAR);
		PRINT'=========================================';

	END CATCH
	
END


--EXEC silver.load_silver