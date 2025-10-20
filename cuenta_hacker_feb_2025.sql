CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_feb` as 
(
with base_inicial as 
(select 
    res.USER_ID,
    res.INFRACTION_TYPE,
    res.SENTENCE_DATE,
    res.SIT_SITE_ID,
    res.SENTENCE_LAST_STATUS,
    res.SENTENCE_REASON_REHABILITATION,
    cus.CUS_RU_SINCE_DT,
    abs(date_diff(cast(CUS_RU_SINCE_DT as date),current_date,day)) as dias_cuenta_activa
FROM 
    `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
LEFT JOIN 
    `meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA` cus 
  ON res.USER_ID = cus.CUS_CUST_ID
where 1=1
AND res.infraction_type = 'CUENTA_DE_HACKER'
AND extract(year from cast(res.sentence_date as date)) = 2025
AND extract(month from cast(res.sentence_date as date)) = 02
--AND cast(cus.CUS_RU_SINCE_DT as date) <=  cast('2025-02-01' as date)
),

trxs as 
(
SELECT 
    id_contraparte,
    COUNT(distinct bq.operation_id) AS cant_trans, 
    round(sum(bq.op_amt),2) as monto_marcado
FROM 
    `SBOX_PFFINTECHATO.resumen_operaciones` a 
inner join 
    base_inicial b 
  on cast(a.id_contraparte as string) = cast(b.user_id as string)
left join 
   `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
     on a.id_operacion = bq.operation_id
     and bq.status_id = 'A'
     and bq.contramarca = 0
WHERE 1=1
and id_contraparte NOT IN ('No es pago') 
GROUP BY id_contraparte
)

select 
    a.*,
    b.cant_trans,
    b.monto_marcado
from 
    base_inicial a 
left join 
    trxs b 
  on cast(a.user_id as string) = cast(b.id_contraparte as string)
)