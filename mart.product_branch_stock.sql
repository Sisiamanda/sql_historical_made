with 
---------------------------------- sales and physical stock ----------------------------------
sales_and_physical_stock as (
	select 
		product as product_code, 
		branch as branch_code, 
		available_spot as sales_stock ,
		physical as physical_stock
	from raw_abu_prod_ent.current_stock_dc_view 
    	
	union all 
	
	select 
		product as product_code,
		branch as branch_code, 
		available_spot as sales_stock,
		physical as physical_stock
	from raw_abu_prod_ent.current_stock_branch_view 
),

---------------------------------- intransit stock ----------------------------------
intransit_stock as (
	select 
		pp.default_code as product_code, 
		rb.code as branch_code, 
		quantity as transit_quantity
    from raw_abu_prod_ent.stock_quant sq 
    left join raw_abu_prod_ent.stock_location sl on sq.location_id = sl.id
    left join raw_abu_prod_ent.product_product pp on sq.product_id = pp.id 
    left join raw_abu_prod_ent.res_branch rb on rb.id = sl.branch_id 
    where 1=1
    	and sl.is_transit_so is true 
    	or sl.is_transit_to is true
    
),

sum_intransit_stock as (
	select 
		product_code, 
		branch_code, 
		sum(transit_quantity) as transit_quantity
	from intransit_stock
	group by 1,2
),
---------------------------------- staging stock quantity ----------------------------------
staging_stock as (
	select 
			pp.default_code as product_code, 
			rb.code as branch_code, 
			sq.quantity as staging_quantity
	    from raw_abu_prod_ent.stock_quant sq 
	    left join raw_abu_prod_ent.stock_location sl on sq.location_id = sl.id
	    left join raw_abu_prod_ent.product_product pp on sq.product_id = pp.id 
	    left join raw_abu_prod_ent.res_branch rb on rb.id = sl.branch_id 
	    where 1=1
	    	and sl."usage" = 'internal'
	    	and sl.is_staging_adjustment is true
			or sl.is_staging_combine is true
			or sl.is_staging_fg is true
			or sl.is_staging_po is true
			or sl.is_staging_production is true
			or sl.is_staging_qc is true
			or sl.is_staging_return is true
			or sl.is_staging_rma is true
			or sl.is_staging_so is true
			or sl.is_staging_to is true
),

sum_staging_stock as (
	select 
		product_code, 
		branch_code, 
		sum(staging_quantity) as staging_quantity
	from staging_stock
	group by 1,2
),

---------------------------------- purchase order quantity ----------------------------------
po_stock_final as (
select 
	product_code,
	branch_code,
	purchase_quantity_po
from mart.sum_abu_purchase_order_outstanding
),

---------------------------------- union product code ----------------------------------
cte_union_product_code as (
--)
	select 
		product_code,
		branch_code
	from sales_and_physical_stock -- take product code physical stock
	
	union 
	
	select 
		product_code,
		branch_code
	from sum_intransit_stock -- take product code intransit_stock
	group by 1,2
	
	union
	
	select 
		product_code,
        branch_code
    from po_stock_final -- take product code purchase order
    
    union 
    
    select 
    	product_code,
    	branch_code
    from mart.fact_abu_pnj_odoo_ax_union_invoice_quantity -- take product code quantity invoiced
	group by 1,2
	
	union
	
	select 
		product_code, 
		branch_code
	from sum_staging_stock -- take product code staging stock
	where product_code like 'P%'
	group by 1,2
	
),

---------------------------------- sales union from (ABU AX), (ABU ODOO), (PNJ ODOO) ----------------------------------
sales_abu_pnj_ax_odoo as (
	select 
		product_code,
		branch_code,
		invoice_date,
		quantity_invoiced,
		data_type
	from mart.fact_abu_pnj_odoo_ax_union_invoice_quantity
),

sales_abu_odoo as (
	select
	    sapao.product_code,
	    sapao.branch_code,
	    case 
	    	when sapao.invoice_date >= current_date - interval '7 days' and sapao.invoice_date <= current_date - interval '1 days' 
	    	then sum(sapao.quantity_invoiced)/7 else 0
	    end as sales_7,
	    case  
			when sapao.invoice_date >= current_date - interval '14 days' and sapao.invoice_date <= current_date - interval '1 days' 
			then SUM(sapao.quantity_invoiced)/14 else 0 
		end as sales_14,
		case  
			when sapao.invoice_date >= current_date - interval '30 days' and sapao.invoice_date <= current_date - interval '1 days' 
			then SUM(sapao.quantity_invoiced)/30 else 0 
		end as sales_30,
		case  
			when sapao.invoice_date >= current_date - interval '60 days' and sapao.invoice_date <= current_date - interval '1 days' 
			then SUM(sapao.quantity_invoiced)/60 else 0 
		end as sales_60,
		case  
			when sapao.invoice_date >= current_date - interval '90 days' and sapao.invoice_date <= current_date - interval '1 days' 
			then SUM(sapao.quantity_invoiced)/90 else 0 
		end as sales_90,
		case  
			when sapao.invoice_date >= current_date - interval '180 days' and sapao.invoice_date <= current_date - interval '1 days' 
			then SUM(sapao.quantity_invoiced)/180 else 0 
		end as sales_180    
	from sales_abu_pnj_ax_odoo sapao
 	group by sapao.product_code, sapao.branch_code, sapao.invoice_date
 ),
 
 sum_sales_abu_odoo as (
 	SELECT 
	    sao.product_code,
	    sao.branch_code,
		SUM(sao.sales_7) as avg_day_7, 
		SUM(sao.sales_14) as avg_day_14,
		SUM(sao.sales_30) as avg_day_30,
		SUM(sao.sales_60) as avg_day_60,
		SUM(sao.sales_90) as avg_day_90,
    	SUM(sao.sales_180) as avg_day_180
	FROM sales_abu_odoo as sao
	GROUP by 1,2
	
),

---------------------------------- product category ----------------------------------
product_name as (
	select 
		default_code as product_code,
		product_category,
		sub_category,
		REPLACE(cast(product_name as text), '"', '') as product_name
	from mart.fact_master_product_commercial
),


---------------------------------- last purchase order date ----------------------------------
last_po_date_abu as (
	select 
		pp.default_code as product_code, 
		rb.code as branch_code, 
		max(sp.date_done + interval '7 hours') as last_po_date
	from raw_abu_prod_ent.purchase_order_line pol 
	left join raw_abu_prod_ent.purchase_order po on po.id = pol.order_id 
	left join raw_abu_prod_ent.product_product pp on pp.id = pol.product_id
	left join raw_abu_prod_ent.res_branch rb on rb.id = pol.branch_id 
	left join raw_abu_prod_ent.stock_picking sp on sp.origin = po."name" 
		and sp.state = 'done'
	where left(pp.default_code,1) = 'P' 
		and rb.code is not null
	group by 1,2
	
),

last_po_date_pnj as (
	select
		pp.default_code as product_code,
		rb.code as branch_code,
		max(sml.date) as last_po_date
	from raw_pnj_prod.stock_move_line as sml
	left join raw_pnj_prod.product_product as pp on sml.product_id = pp.id
	left join raw_pnj_prod.stock_location as sl on sml.location_dest_id = sl.id
	left join raw_pnj_prod.res_branch as rb on sl.branch_id = rb.id
	where sml.location_id = '8'
	and pp.default_code like 'P%'
	group by 1,2
),

po_date_union as (
	select 
		product_code,
		branch_code,
		cast(last_po_date as date) as last_po_date
	from last_po_date_abu
	
	union 
	
	select 
		product_code,
		branch_code,
		last_po_date
	from last_po_date_pnj
),

cte_last_po_date_condition as (
	select
		product_code,
		branch_code,
		last_po_date,
		ROW_NUMBER() OVER (PARTITION BY product_code, branch_code ORDER BY last_po_date DESC) AS row_num
	from po_date_union
),

last_po_date_final as (
	select
		product_code,
		branch_code,
		last_po_date
	from cte_last_po_date_condition
	where 1=1
		and row_num = 1
),

---------------------------------- last sale order date ----------------------------------
last_so_date_abu_join as (
	select 
		product_code,
		branch_code,
		max(invoice_date) as last_so_date
	from mart.fact_abu_pnj_odoo_ax_union_invoice_quantity
	where 1=1 
	group by 1,2

	union 
	
	select
		pp.default_code as item_id,
		rb.code as branch,
		max(sml.date) as last_so_date
	from raw_pnj_prod.stock_move_line as sml
	left join raw_pnj_prod.product_product as pp on sml.product_id = pp.id
	left join raw_pnj_prod.stock_location as sl on sml.location_id = sl.id
	left join raw_pnj_prod.res_branch as rb on sl.branch_id = rb.id
	where sml.location_dest_id = '9'
		and pp.default_code like 'P%'
	group by 1,2
),

last_so_date_abu_condition as (
	select 		
		product_code,
		branch_code,
		last_so_date,
		row_number() over (partition by product_code, branch_code order by last_so_date desc) as row_num
	from last_so_date_abu_join
),

last_so_date_final as (
	select 
		product_code,
		branch_code,
		last_so_date
	from last_so_date_abu_condition
	where row_num = 1
),

---------------------------------- last transfer order date ----------------------------------
last_to_date_join as (
	select 
		pp.default_code as product_code, 
		rb.code as branch_code, 
		to2.eta as last_to_date_origin,
		to2.status as transfer_status,
		case 
			when to2.status not in ('received','done')  then null --take only TO date with status 'received' and ready to sold
			else to2.eta
		end as last_to_date 
	from  raw_abu_prod_ent.merge_transfer_order_line mtol 
	left join raw_abu_prod_ent.transfer_order to2 on to2.id = mtol.transfer_order_id 
	left join raw_abu_prod_ent.res_branch rb on rb.id = to2.destination_branch_id 
	left join raw_abu_prod_ent.product_product pp on pp.id = mtol.product_id 
	where 1=1

),

last_to_date_condition as (
	select 
		product_code, 
		branch_code, 
		max(last_to_date) as last_to_date
	from last_to_date_join
	where 1=1 
		and transfer_status = 'shipped'
	group by 1,2
),

---------------------------------- join all ----------------------------------
all_join as (
	select 
		upc.product_code,
		upc.branch_code, 
		saps.sales_stock ,
		saps.physical_stock,
		ins.transit_quantity,
		sss.staging_quantity,
		ps.purchase_quantity_po,
		ssao.avg_day_7, 
		ssao.avg_day_14,
		ssao.avg_day_30,
		ssao.avg_day_60,
		ssao.avg_day_90,
	    ssao.avg_day_180,
    	pn.product_category,
    	pn.sub_category,
    	pn.product_name,
    	lpdf.last_po_date,
    	lsdf.last_so_date,
    	ltdc.last_to_date
	from cte_union_product_code upc
	left join sales_and_physical_stock as saps on upc.product_code = saps.product_code
		and upc.branch_code = saps.branch_code
	left join sum_intransit_stock as ins on upc.product_code = ins.product_code
		and upc.branch_code = ins.branch_code
	left join po_stock_final as ps on upc.product_code = ps.product_code
		and upc.branch_code = ps.branch_code
	left join sum_sales_abu_odoo as ssao on upc.product_code = ssao.product_code
		and upc.branch_code = ssao.branch_code
	left join product_name as pn on upc.product_code = pn.product_code
		left join last_po_date_final as lpdf on upc.product_code = lpdf.product_code
		and upc.branch_code = lpdf.branch_code
	left join last_so_date_final as lsdf on upc.product_code =  lsdf.product_code
		and upc.branch_code = lsdf.branch_code
	left join last_to_date_condition ltdc on upc.product_code = ltdc.product_code
		and upc.branch_code = ltdc.branch_code
	left join sum_staging_stock as sss on upc.product_code = sss.product_code
		and upc.branch_code = sss.branch_code	
	where 1=1
),

all_join_condition as (
	select 
		branch_code,
		product_code,
		product_name,
		coalesce(physical_stock,0) as physical_stock,
		coalesce(sales_stock,0) as sales_stock,
		coalesce(transit_quantity,0) as transit_quantity,
		coalesce(staging_quantity,0) as staging_quantity,
		coalesce(purchase_quantity_po,0) as purchase_quantity_po,
		product_category,
		sub_category,
		coalesce(avg_day_7,0) as avg_day_7, 
		coalesce(avg_day_14,0) as avg_day_14,
		coalesce(avg_day_30,0) as avg_day_30,
		coalesce(avg_day_60,0) as avg_day_60,
		coalesce(avg_day_90,0) as avg_day_90,
	    coalesce(avg_day_180,0) as avg_day_180,
	    last_po_date,
	    last_so_date,
	    last_to_date
	from all_join 

---------------------------------- final_table_for_svd_and_slowmo ----------------------------------
)
select 
	ajc.branch_code::varchar
	, ajc.product_code::varchar
	, ajc.product_name::varchar
	, ajc.product_category::varchar
	, ajc.sub_category::varchar as product_sub_category
	, ajc.physical_stock as physical_stock_quantity
	, ajc.sales_stock as sale_stock_quantity
	, ajc.staging_quantity as staging_quantity
	, ajc.transit_quantity as in_transit_quantity
	, ajc.purchase_quantity_po as purhase_order_quantity
	, ajc.avg_day_7 as average_sale_7day_quantity
	, ajc.avg_day_14 as average_sale_14day_quantity
	, ajc.avg_day_30 as average_sale_30day_quantity
	, ajc.avg_day_60 as average_sale_60day_quantity
	, ajc.avg_day_90 as average_sale_90day_quantity
	, ajc.avg_day_180 as average_sale_180day_quantity
	, ajc.last_po_date as last_purchase_order_date
	, ajc.last_so_date as last_sale_order_date
	, ajc.last_to_date as last_transfer_order_date
	, cast(extract(day from ajc.last_po_date) - extract(day from ajc.last_so_date) as int)  as lead_time_so_po_in_day --diff_days
	, (case 
		when ajc.last_so_date is not null and ajc.last_po_date is not null then cast(extract(day from ajc.last_so_date) - extract(day from ajc.last_po_date) as VARCHAR)
		else 'no po' --diff_days_from_po
	  end) as lead_time_po_so_in_day
	, (case
		when (ajc.last_so_date < ajc.last_po_date) and (ajc.last_so_date < ajc.last_to_date) then 'new stock'
		else 'old stock'
	   end) as stock_condition
	, (case
		when ajc.avg_day_60 <= 0 then 'no sales'
		else 'sales'
	   end) as sale_condition
	, ajc.physical_stock / nullif(ajc.avg_day_60,0) as day_of_inventory --stock_days_phy
	, ajc.sales_stock / nullif(ajc.avg_day_60,0) as day_of_supply --stock_days_sale
	, (case 
		when (ajc.sales_stock / nullif(ajc.avg_day_60,0)) is null then 9999999
		else (ajc.sales_stock / nullif(ajc.avg_day_60,0))
	end) as day_of_supply_with_overflow --order_flag
	, row_number() over(order by ajc.branch_code, ajc.product_code) as row_number --slowmo_rank (rown_number based order by branch_code then product_code
from all_join_condition as ajc
where 1=1
	and product_category is not null