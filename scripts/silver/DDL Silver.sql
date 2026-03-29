/* 
=============================================================================
DDL Scripts : Create Silver Tables
=============================================================================

Script Purpose: 
	This Script created tables in 'silver' schema, dropping existing tables
	if they already exist.
	Run this script to re-define the DDL structure of 'silver' tables.
==============================================================================

*/
if OBJECT_ID ('silver.crm_cust_info','U') is not null  
	Drop Table silver.crm_cust_info;

create table silver.crm_cust_info(
	cst_id Int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()

);

if OBJECT_ID ('silver.crm_prd_info','U') is not null
	Drop Table silver.crm_prd_info;
Create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);


if OBJECT_ID ('silver.crm_sales_details','U') is not null
	Drop Table silver.crm_sales_details;
Create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_cust_az12','U') is not null
	Drop table silver.erp_cust_az12;
Create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_loc_a101','U') is not null
	Drop Table silver.erp_loc_a101; 
Create table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID ('silver.erp_px_cat_g1v2','U') is not null
	Drop Table silver.erp_px_cat_g1v2;
Create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()
);