-- =========================================================
-- Customer Duplicates Check
-- =========================================================
select cst_id ,count(*) 
from (
	select 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca 
		on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
		on ci.cst_key = la.cid
) t 
group by cst_id 
having count(*) > 1 ;


-- =========================================================
-- Customer Gender Consistency Check
-- =========================================================
select distinct
	ci.cst_gndr,
	ca.gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca 
	on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
	on ci.cst_key = la.cid ;


-- =========================================================
-- Product Duplicates Check
-- =========================================================
select prd_key ,count(*) 
from (
	select 
		pn.prd_id,
		pn.cat_id ,
		pn.prd_key,
		pn.prd_nm ,
		pn.prd_cost ,
		pn.prd_line ,
		pn.prd_start_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	from silver.crm_prd_info pn 
	left join silver.erp_px_cat_g1v2 pc 
		on pn.cat_id = pc.id
	where pn.prd_end_dt is null -- استبعاد المنتجات المنتهية
) t 
group by prd_key
having COUNT(*) > 1 ;


-- =========================================================
-- Foreign Key Integrity Check
-- =========================================================
select * 
from gold.fact_sales f
left join gold.dim_customers c
	on c.customer_key = f.customer_key
left join gold.dim_products p
	on p.product_key = f.product_key
where c.customer_key is null 
   or p.product_key is null ;
