
create or alter procedure silver.load_silver as
begin
      DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
    begin try
            SET @batch_start_time =GETDATE();
                Print '=========================================================';
				PRINT 'Loading Bronze Layer';
				Print '=========================================================';

				PRINT '---------------------------------------------------------';
				PRINT 'LOading CRM Tables';
				PRINT '---------------------------------------------------------';
            
            SET @start_time=GETDATE()
            print '>> truncating table silver.crm_cust_info'
            truncate table silver.crm_cust_info;
            print '>> Inserting Data into : silver.crm_cust_info'
            insert into silver.crm_cust_info(
                    cst_id,
                    cst_key,
                    cst_firstname,
                    cst_lastname,
                    cst_marital_status,
                    cst_gndr,
                    cst_create_date)
            select 
            cst_id,
            cst_key,
            TRIM(cst_firstname)as cst_firstname,
            TRIM(cst_lastname) as cst_lastname,
            case 
                     when upper(TRIM(cst_material_status))= 'S' then 'Single'
                     when Upper(TRIM(cst_material_status))= 'M' then 'Married'
                     else 'N/o'
              end cst_material_status,
              case 
                     when upper(TRIM(cst_gndr))= 'F' then 'Female'
                     when Upper(TRIM(cst_gndr))= 'M' then 'Male'
                     else 'N/o'
              end cst_gndr,
            cst_create_date
            from (
            select * ,
            ROW_NUMBER()over(partition by cst_id order by cst_create_date desc)as flag_last
            from bronze.crm_cust_info)flaged
            where flag_last=1 ;
            SET @end_time=GETDATE();
						PRINT'>> Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
						PRINT'>>--------------------------------------';



            -- table silver.crm_prd_info after transformations --
            SET @start_time=GETDATE();
            print '>> truncating table silver.crm_prd_info'
            truncate table silver.crm_prd_info;
            print '>> Inserting Data into : silver.crm_prd_info'
            insert into silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
            )
            select
            prd_id,
            REPLACE (SUBSTRING(prd_key,1,5),'-','_')as cat_id,
            SUBSTRING(prd_key,7,LEN(prd_key))as prd_key,
            prd_nm,
            isnull(prd_cost,0)as prd_cost,
            case 
		            when upper(trim(prd_line))='M' then 'Mountain'
		            when upper(trim(prd_line))='R' then 'Road'
		            when upper(trim(prd_line))='M' then 'Other Sales'
		            when upper(trim(prd_line))='M' then 'Touring'
		            else 'n/a'
            end prd_line,
            cast(prd_start_dt as DATE)as prd_start_dt,
            cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as DATE) as prd_end_dt
            from  bronze.crm_prd_info;
            SET @end_time=GETDATE();
						PRINT'>> Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
						PRINT'>>--------------------------------------';




            -- sales_table transformations 
            SET @start_time=GETDATE();
            print '>> truncating table silver.crm_sale_details'
            truncate table silver.crm_sale_details;
            print '>> Inserting Data into : silver.crm_sale_details'
            insert into silver.crm_sale_details(
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

            select 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,


            case 
                when sls_order_dt=0 or LEN(sls_order_dt)<8 then null
                else cast(cast(sls_order_dt as varchar)as date)
            end as sls_order_dt,
            case 
                when sls_ship_dt=0 or LEN(sls_ship_dt)<8 then null
                else cast(cast(sls_ship_dt as varchar)as date)
            end as sls_ship_dt,
            case 
                when sls_due_dt=0 or LEN(sls_due_dt)<8 then null
                else cast(cast(sls_due_dt as varchar)as date)
            end as sls_due_dt,

             case
		            when sls_sales is null or sls_sales <0 or sls_sales=0 
		            then sls_quantity * abs(sls_price)
		            else sls_sales
            end as sls_sales,
            sls_quantity,
             case 
		            when sls_price=0 or sls_price is null then sls_sales/ nullif(sls_quantity,0)
		            when sls_price <0 then ABS(sls_price)
		            else sls_price
            end as sls_price
            from bronze.crm_sale_details;
            SET @end_time=GETDATE();
						PRINT'>> Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
						PRINT'>>--------------------------------------';

                PRINT '---------------------------------------------------------';
				PRINT 'LOading ERP Tables';
				PRINT '---------------------------------------------------------';

            -- ERp tables transformations
            SET @start_time=GETDATE();
            print '>> truncating table silver.erp_cust_az12'
            truncate table silver.erp_cust_az12;
            print '>> Inserting Data into : silver.erp_cust_az12'
            insert into  silver.erp_cust_az12(
            cid,
            bdate,
            gen

            )
            select 
            case 
		            when cid like 'NAS%'then SUBSTRING(cid,4,LEN(cid))
		            else cid
            end as cid,
            case 
                    when bdate > GETDATE() then null
                    else bdate
            end as bdate,

            case 
                    when UPPER(TRIM(gen)) in('F','FEMALE') then 'Female'
                    when UPPER(TRIM(gen)) in('M','MALE') then 'Male'
                    else 'n/a'
            end as gen
            from bronze.erp_cust_az12;
            SET @end_time=GETDATE();
						PRINT'>> Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
						PRINT'>>--------------------------------------';



            SET @start_time=GETDATE();
            print '>> truncating table silver.erp_loc_a101'
            truncate table silver.erp_loc_a101;
            print '>> Inserting Data into : silver.erp_loc_a101'
            insert into silver.erp_loc_a101(cid,cntry)
            select replace(cid,'-','') cid,
            case
		            when TRIM(cntry)='DE' then 'Germany'
		            when TRIM(cntry) in ('US','USA') then 'United States'
		            when cntry is null or TRIM(cntry)='' then 'N/a'
		            else cntry
            end as cntry 
            from bronze.erp_loc_a101;
            SET @end_time=GETDATE();
						PRINT'>> Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
						PRINT'>>--------------------------------------';



            SET @start_time=GETDATE();
            print '>> truncating table silver.erp_px_cat_g1v2'
            truncate table silver.erp_px_cat_g1v2;
            print '>> Inserting Data into : silver.erp_px_cat_g1v2'
            insert into silver.erp_px_cat_g1v2(
            id,cat,subcat,maintenance)
            select id,
            cat,
            subcat,
            maintenance
            from bronze.erp_px_cat_g1v2;
            SET @end_time=GETDATE();
						PRINT'>> Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
						PRINT'>>--------------------------------------';
    
            SET @batch_end_time=GETDATE();
						        PRINT '=====================================================';
						        PRINT'LOading Bronze Layer is Completed'
						        PRINT'>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
						        PRINT '=====================================================';
    end try
    begin catch
		PRINT '=====================================================';
		PRINT ' ERROR DURING LOADING  BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_MESSAGE()AS NVARCHAR );
		PRINT '=====================================================';
	end catch

end
