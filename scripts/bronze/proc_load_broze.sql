/*
========================================================================================================

Stored Procedure: Load Bronze Layer (Source -> Bronze)

========================================================================================================

Script Purpose:
	This script procedure loads data into the'bronze' schema from external csv files.
	It performs the following actions:
		- Truncates the bronze tables before loading the data.
		- Uses the 'bulk insert' command to load data from csv files to bronze tables.

	Parameters:
		None
	 This Stored Procedure does not accept any parameters or return any values.

	 Usage Example		
		EXEC bronze.load_bronze;

========================================================================================================

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_End_time DATETIME;
	BEGIN TRY
		SET @batch_start_time= GETDATE();
		PRINT '=============================================';
		PRINT 'Loading Bronze Layer'
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info ;

		PRINT '>> Inserting Data into: bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		From 'C:\DataWarehouse\source_crm\cust_info.csv'
		with(
			Firstrow=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
        SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time ) As NVARCHAR) + 'seconds'
		PRINT '---------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data into: bronze.crm_prd_info ';
		Bulk insert bronze.crm_prd_info
		from 'C:\DataWarehouse\source_crm\prd_info.csv'
		with (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			);
        SET @end_time= GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------';

		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into: bronze.crm_sales_details ';
		Bulk insert bronze.crm_sales_details
		from 'C:\DataWarehouse\source_crm\sales_details.csv'
		with (FIRSTROW=2,
			  FIELDTERMINATOR=',',
			  TABLOCK
			  );
		SET @end_time= GETDATE();

		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '--------------';


        PRINT '---------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------';
		
		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data into: bronze.erp_cust_az12 ';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\DataWarehouse\source_erp\CUST_AZ12.csv'
		with (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			);

        SET @end_time= GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '--------------';


		SET @start_time= GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data into: bronze.erp_loc_a101 ';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\DataWarehouse\source_erp\LOC_A101.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------';

		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2 ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\DataWarehouse\source_erp\PX_CAT_G1V2.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK
			);
		SET @end_time= GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)+ 'seconds';
		PRINT '--------------';

		SET @batch_End_time= GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_End_time) AS NVARCHAR) + 'seconds'
	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'Error Ocurred During Loading Bronze Layer';
		PRINT 'Error Message' +  ERROR_MESSAGE(); 
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR); 
		PRINT '=========================================';

	END CATCH
END