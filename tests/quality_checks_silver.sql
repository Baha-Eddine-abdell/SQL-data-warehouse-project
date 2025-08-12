/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- Understand the data --
SELECT TOP  1000 * FROM bronze.crm_cust_info;
SELECT TOP  1000 * FROM bronze.crm_prd_info;
SELECT TOP  1000 * FROM bronze.crm_sale_details;
SELECT TOP  1000 * FROM bronze.erp_cust_az12;
SELECT TOP  1000 * FROM bronze.erp_loc_a101;
SELECT TOP  1000 * FROM bronze.erp_px_cat_g1v2;

-- data Quality check --

-- Check for nulls or duplicate in Primary Key--
 -- Expectation :no  REsult
select cst_id , count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) >1 or cst_id is NULL

-- check for unwanted spaces--
-- Expectation: no result 
-- result : 15 columns
select cst_firstname 
from bronze.crm_cust_info
where cst_firstname!= TRIM(cst_firstname);


-- DAta Standardization & Consistency
select DIStinct cst_gndr from bronze.crm_cust_info;
-- REsult : 3 categories 
select DISTINCT cst_material_status from bronze.crm_cust_info;


-- data qualty check for the table bronze.crm_prd_info

select prd_id,count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id Is NULL;



select prd_cost 
from bronze.crm_prd_info
where prd_cost <0 or prd_cost is null ;

-- table bronze.crm_sale_details --

-- check for unwanted spaces
select * from bronze.crm_sale_details
where sls_ord_num != TRIM(sls_ord_num);


-- check for connection between the tables using the prd_key
select * 
from bronze.crm_sale_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info );

-- check the quality for the columns order_dt, ship _Dt 
-- dates column cant have 0 values or negative values
--check for outliers by validating the boundaries of the date range 
select NULLIF(sls_order_dt,0) 
from bronze.crm_sale_details
where  sls_order_dt <0 or len(sls_order_dt) !=8 or sls_order_dt>20500101 or sls_order_dt<20100101;


-- check for ship_dt values vs order_dt values (order_dt < ship_dt)

select * from bronze.crm_sale_details
where sls_ship_dt< sls_order_dt or sls_order_dt > sls_due_dt;

-- check for the consistency of the values in sales column
-- values must not be null,negative or zero
-- sales =quantity * price
select distinct  sls_price,sls_quantity,sls_sales 
from bronze.crm_sale_details
where  sls_sales != sls_price * sls_quantity or
sls_sales is null or sls_quantity is null or sls_price is null
 or sls_sales<=0 or sls_quantity<=0or sls_price <=0
 order by sls_sales,sls_price,sls_quantity;


 -- the rules used for fixing the bad data quality are :
 --if sales is negative,zero or null , derive it using quantity and price
 --if price is zero or null ,calculate it using sales and quantity
 --if price is negative, convert it to positive value
 select distinct 
 sls_price as old_price,
 case 
		when sls_price=0 or sls_price is null then sls_sales/ nullif(sls_quantity,0)
		when sls_price <0 then ABS(sls_price)
		else sls_price
end as sls_price,
 sls_quantity,
 sls_sales as old_sales,
 case
		when sls_sales is null or sls_sales <0 or sls_sales=0 
		then sls_quantity * abs(sls_price)
		else sls_sales
end as sls_sales
from bronze.crm_sale_details
where  sls_sales != sls_price * sls_quantity or
sls_sales is null or sls_quantity is null or sls_price is null
 or sls_sales<=0 or sls_quantity<=0or sls_price <=0
 order by sls_sales,sls_price,sls_quantity;

 select * from silver.crm_sale_details


 -- erp tables data quality checks --

 select * from bronze.erp_cust_az12
 where cid like '%AW00011000';
 
-- the cid values is like the cst_key in crm_cust_info table but with the difference 'NAS'

select cid as old_cid,
case 
		when cid like 'NAS%'then SUBSTRING(cid,4,LEN(cid))
		else cid
end as cid,
bdate,gen 
from bronze.erp_cust_az12;








-- data quality of the birthdate column
select  distinct bdate
from bronze.erp_cust_az12
where bdate<'1924-01-01' or bdate>GETDATE();


-- check the gen column
select distinct gen from bronze.erp_cust_az12


select * from silver.erp_cust_az12;


-- location table --
select replace(cid,'-','') from bronze.erp_loc_a101
where replace(cid,'-','') not in  (select cst_key from silver.crm_cust_info);

select distinct cntry from bronze.erp_loc_a101
order by cntry;
select 
case
		when TRIM(cntry)='DE' then 'Germany'
		when TRIM(cntry) in ('US','USA') then 'United States'
		when cntry is null or cntry='' then 'N/a'
		else cntry
end as cntry 
from bronze.erp_loc_a101;


--erp_px_cat_g table
select * from bronze.erp_px_cat_g1v2

select * from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance)

select distinct cat from bronze.erp_px_cat_g1v2;
