/*******************************************
   Cantidad de transacciones (TPN),
   Monto de las transacciones en USD (TPV),
   Users de ATO y DTO confirmados
   Site MLB
********************************************/

Select 
format_date('%Y-%m',op_dt) as mes_denuncia,
site_id AS site,
cust_id as usuario_denunciante,
tipo_robo as tipo_robo,
sum(op_amt) as monetizacion_mensual,
count(operation_id) as TPN
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
where 1=1
and site_id = 'MLB'
and op_dt >= '2024-01-01' 
and contramarca = 0
and status_id = 'A'
and flow_type in ('MO','PO','MT','MI','MC')
group by 1, 2, 3, 4
order by 1, 2, 3, 4
-- 13523 transacciones monetizadas
-- 50346 TPN
-- Monto de las transacciones en USD a traves de monetizacion_mensual
-- Info de ATO/DTO confirmados a traves de tipo_robo pero se filtra por contramarca 0 y status_id A.
-- Informacion de 2024 y por site MLB.
;

-------------------------------------------------------------------

/*******************************************
   Cantidad de transacciones (TPN),
   Monto de las transacciones en USD (TPV),
   Users de ATO y DTO confirmados
   Site MLA
********************************************/

Select 
format_date('%Y-%m',op_dt) as mes_denuncia,
site_id AS site,
cust_id as usuario_denunciante,
tipo_robo as tipo_robo,
sum(op_amt) as monetizacion_mensual,
count(operation_id) as TPN
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
where 1=1
and site_id = 'MLA'
and op_dt >= '2024-01-01' 
and contramarca = 0
and status_id = 'A'
and flow_type in ('MO','PO','MT','MI','MC')
group by 1, 2, 3, 4
order by 1, 2, 3, 4
-- 9112 transacciones monetizadas
-- 35012 TPN
-- Monto de las transacciones en USD a traves de monetizacion_mensual
-- Info de ATO/DTO confirmados a traves de tipo_robo pero se filtra por contramarca 0 y status_id A.
-- Informacion de 2024 y por site MLA.
;
