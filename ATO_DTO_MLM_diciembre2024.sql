select *
from `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
where 1=1
and site_id = 'MLM'
and extract(year from cast(op_dt as date)) = 2024
and extract(month from cast(op_dt as date)) = 12
and status_id = 'A'
and contramarca = 0
--group by 1
--order by 2 desc
;