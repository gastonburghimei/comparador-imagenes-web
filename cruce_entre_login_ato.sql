/* Query que tiene cruce resuelto entre login y ato. 
   La row existe si hubo cruce de device_id entre el login y la transaccion marcada con ATO. 
   No descrimina si monetizo o no. */

select tmp.*,
coalesce(bq.cant_monetizacion,0) as cant_monetizacion
from `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` tmp
    inner join (select
   cust_id,
   count(distinct operation_id) as cant_monetizacion
   from `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
   where 1=1
   and lower(status_id) = 'a'
   and contramarca = 0
   group by 1 
   order by cust_id) bq
    on tmp.cust_id = bq.cust_id
where 1=1
--and op_datetime > '2024-10-01' 
--1900 transacciones
and extract(year from cast(tmp.op_datetime as date)) = 2024
and extract(month from cast(tmp.op_datetime as date)) = 10
--1299 transacciones
and tmp.client_type = 'webview'
;