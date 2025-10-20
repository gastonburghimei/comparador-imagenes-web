SELECT * FROM `meli-bi-data.SBOX_PFFINTECHATO.ATO_LogIn` 
LIMIT 10
;
-- cust_id , logintime_GMT_3_ , op_datetime

SELECT * FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
where 1=1
and marca_ato = 1
and contramarca = 0
and extract(year from cast(op_dt as date)) = 2024
and extract(month from cast(op_dt as date)) = 10
;
-- Esto es para traerme los casos confirmados de ATO en octubre 2024
-- Con estos filtros me da que son 19403 lo cual me parece un monton

------------------------------------------------------------------------
-- Ahora voy a cruzar las tablas para ver si hay algun ATO confirmado

select
ali.cust_id,
--ali.op_datetime as fecha,
bq.op_dt as fecha,
bq.site_id as site,
bq.status_id as status,
bq.op_amt as monetizacion,
bq.marca_ato,
bq.tipo_robo,
from `meli-bi-data.SBOX_PFFINTECHATO.ATO_LogIn` ali 
    left join `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq 
    on ali.cust_id = bq.cust_id
where 1=1
and bq.marca_ato = 1
and bq.contramarca = 0
and extract(year from cast(bq.op_dt as date)) = 2024
and extract(month from cast(bq.op_dt as date)) = 10
and bq.status_id = 'A'
--Cuando aplico el filtro de status_id me quedan 463 casos
--and bq.site_id = 'MLA'
;

---------------------------------------------------------------
-- Ahora estoy testeando la otra tabla de consultas
select *
from `meli-bi-data.SBOX_PF_PAY_METRICS.CONSULTAS_ATO`
where 1=1
and gca_status = 'open'
and marcaje_ato = 0
--and extract(year from cast(fecha_apertura_caso as date)) = 2024
--and extract(month from cast(fecha_apertura_caso as date)) = 10
-- No se encuentran casos que sigan abiertos, sin confirmacion de ato para octubre 2024
;

-----------------------------------------------------------------

select
ali.cust_id,
ca.GCA_CUST_ID,
ca.gca_status,
ca.marcaje_ato,
ca.fecha_apertura_caso
from `meli-bi-data.SBOX_PFFINTECHATO.ATO_LogIn` ali 
    left join `meli-bi-data.SBOX_PF_PAY_METRICS.CONSULTAS_ATO` ca 
    on cast(ali.cust_id as string) = ca.gca_cust_id
where 1=1
and ca.gca_status = 'open'
and ca.marcaje_ato = 0
--and extract(year from cast(ca.fecha_apertura_caso as date)) = 2024
--and extract(month from cast(ca.fecha_apertura_caso as date)) = 10
--Solo aparece uno con cust_id = 239773705, pero es de. 25.09. No hay de octubre.
;

-----------------------------------------------------------------
-- Aca lo que quiero confirmar es si todos los cust_id en estas
-- condiciones tiene ato confirmado
select
DISTINCT(ali.cust_id)
from `meli-bi-data.SBOX_PFFINTECHATO.ATO_LogIn` ali 
    left join `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq 
    on ali.cust_id = bq.cust_id
where 1=1
and bq.marca_ato = 1
and bq.contramarca = 0
and extract(year from cast(bq.op_dt as date)) = 2024
and extract(month from cast(bq.op_dt as date)) = 10
--and bq.status_id = 'A'
--204 cust_id por lo que el 100% seria un ato confirmado en estas condiciones
;

select * 
from `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` 
where 1=1
and op_datetime > '2024-10-01' 
and client_type = 'webview'
;