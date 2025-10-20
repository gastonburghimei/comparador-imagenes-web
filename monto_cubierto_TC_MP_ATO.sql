--Query creada por Ale Ordoqui

select  
site_id,
format_datetime("%Y-%m",op_dt) as anio_mes,
tarjeta_MP,
count(1) as qty_txs,
round(sum(op_amt),0) as amt_dol,
round(sum(lc_op_amt),0) as amt_lc,
count(distinct cust_id) as qty_users
from `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
where 1=1
and tipo_robo='ATO'
and tarjeta_MP like 'TC%'
AND status_id in ('A','D')
and contramarca=0
group by all
order by anio_mes desc;