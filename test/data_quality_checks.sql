/* =========================================================
   Data Quality Checks - Bronze Layer
   ========================================================= */

-- 1) Country standardization & consistency
select distinct cntry from bronze.erp_loc_a101;

-- 2) Unwanted spaces in ERP categories
select * 
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

-- 3) Standardization checks for category attributes
select distinct cat from bronze.erp_px_cat_g1v2;
select distinct subcat from bronze.erp_px_cat_g1v2;
select distinct maintenance from bronze.erp_px_cat_g1v2;

-- 4) Gender standardization
select distinct gen from bronze.erp_cust_az12;

-- 5) Invalid dates in sales details
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
   or len(sls_order_dt) != 8
   or sls_order_dt > 20250101
   or sls_order_dt < 19000101;

-- 6) Invalid date order (order > ship > due)
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt
   or sls_order_dt > sls_due_dt
   or sls_ship_dt > sls_due_dt;

-- 7) Sales consistency (sales = qty * price)
select sls_sales, sls_quantity, sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
   or sls_sales is null or sls_quantity is null or sls_price is null
   or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0;

/* =========================================================
   Data Quality Checks - Silver Layer
   ========================================================= */

-- 1) Duplicate or null PKs in sales
select sls_ord_num, count(*)
from silver.crm_sales_details
group by sls_ord_num
having count(*) > 1 or sls_ord_num is null;

-- 2) Invalid dates in sales
select sls_order_dt
from silver.crm_sales_details
where sls_order_dt <= 0
   or len(sls_order_dt) != 8
   or sls_order_dt > 20250101
   or sls_order_dt < 19000101;

-- 3) Invalid date order
select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt
   or sls_order_dt > sls_due_dt
   or sls_ship_dt > sls_due_dt;

-- 4) Sales consistency
select sls_sales, sls_quantity, sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
   or sls_sales is null or sls_quantity is null or sls_price is null
   or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0;
