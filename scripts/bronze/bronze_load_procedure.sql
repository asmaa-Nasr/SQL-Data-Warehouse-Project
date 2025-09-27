-- =========================================
-- Procedure: bronze.load_bronze
-- Purpose  : Bulk load raw data into bronze layer
-- =========================================

create or alter procedure bronze.load_bronze as
begin 
    declare @start_time datetime ,@end_time datetime ,@full_start_time datetime ,@full_end_time datetime;
    begin try
		print 'Loading bonze layer'
		set @full_start_time = Getdate()
		set @start_time=GETDATE();
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info
		from 'C:\Users\start\OneDrive\Desktop\DEPI\firstPro\datasets\source_crm\cust_info.csv'
		with(
		 firstrow=2,
		 fieldterminator =',',
		 tablock
		);
		set @end_time=GETDATE();
		print '>> load Duration' +cast( DateDiff(second,@start_time,@end_time)as nvarchar)+ ' seconds';

		set @start_time=GETDATE();
		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info
		from 'C:\Users\start\OneDrive\Desktop\DEPI\firstPro\datasets\source_crm\prd_info.csv'
		with(
		 firstrow=2,
		 fieldterminator =',',
		 tablock
		);
		set @end_time=GETDATE();
		print '>> load Duration' +cast( DateDiff(second,@start_time,@end_time)as nvarchar)+ ' seconds';


		set @start_time=GETDATE();
		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details
		from 'C:\Users\start\OneDrive\Desktop\DEPI\firstPro\datasets\source_crm\sales_details.csv'
		with(
		 firstrow=2,
		 fieldterminator =',',
		 tablock
		);
		set @end_time=GETDATE();
		print '>> load Duration' +cast( DateDiff(second,@start_time,@end_time)as nvarchar)+ ' seconds';


		set @start_time=GETDATE();
		truncate table bronze.erp_cust_az12;
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\start\OneDrive\Desktop\DEPI\firstPro\datasets\source_erp\CUST_AZ12.csv'
		with(
		 firstrow=2,
		 fieldterminator =',',
		 tablock
		);
		set @end_time=GETDATE();
		print '>> load Duration' +cast( DateDiff(second,@start_time,@end_time)as nvarchar)+ ' seconds';

		set @start_time=GETDATE();
		truncate table bronze.erp_loc_a101;
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\start\OneDrive\Desktop\DEPI\firstPro\datasets\source_erp\LOC_A101.csv'
		with(
		 firstrow=2,
		 fieldterminator =',',
		 tablock
		);
		set @end_time=GETDATE();
		print '>> load Duration' +cast( DateDiff(second,@start_time,@end_time)as nvarchar)+ ' seconds';

		set @start_time=GETDATE();
		truncate table bronze.erp_px_cat_g1v2;
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\start\OneDrive\Desktop\DEPI\firstPro\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
		 firstrow=2,
		 fieldterminator =',',
		 tablock
		);
		set @end_time=GETDATE();
		print '>> load Duration' +cast( DateDiff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
		print '====================================================================================='
		set @full_end_time = Getdate()
		print '>>full load Duration' +cast( DateDiff(second,@full_start_time,@full_end_time)as nvarchar)+ ' seconds';
	end try
	begin catch
	    print 'Error occured during loading bronze layer';
		print 'Error Message '+ Error_message();
		end catch
end 

-- =========================================
-- Execute procedure
-- =========================================
EXEC bronze.load_bronze;
