--Actualizar esta tabla cuando tengamos que revisar
--Ultima actualizacion martes 20 de mayo de 2025

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo` as 
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

--Una vez que actualizo la tabla anterior, tiramos esta con el user que necesitamos y actualizando la fecha de pay_move_date
--Demora unos 3 minutos

SELECT 
  h.*,
  ROUND(SUM(PAY_TRANSACTION_DOL_AMT),2) as monto_recibido,
  count(p.PAY_PAYMENT_ID) as cantidad_recibida
from `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo` h
 inner join `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS` p
    on h.user_id = p.CUS_CUST_ID_SEL
    AND p.pay_move_date >= DATE_SUB(h.SENTENCE_DATE, INTERVAL 90 DAY)
    AND p.pay_move_date <= h.SENTENCE_DATE
    and p.pay_move_date >= '2025-01-01' --Actualizar fecha mes a mes
WHERE 1=1
AND p.pay_status_id = 'approved'
and p.tpv_flag = 1
and h.user_id = 1350371466 --Poner user a evaluar
group by 1,2,3,4,5,6,7,8,9,10

--El viernes 4 de abril nos acercaron unos casos que cayeron mal aparentemente pero en octubre 2024 y la query que utilice para chequear fue

SELECT 
  h.*,
  ROUND(SUM(PAY_TRANSACTION_DOL_AMT),2) as monto_recibido,
  count(p.PAY_PAYMENT_ID) as cantidad_recibida
from `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_chequeo` h
 inner join `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS` p
    on h.user_id = p.CUS_CUST_ID_SEL
    AND p.pay_move_date >= DATE_SUB(h.SENTENCE_DATE, INTERVAL 90 DAY)
    AND p.pay_move_date <= h.SENTENCE_DATE
    and p.pay_move_date >= '2024-07-01' --Puse la fecha 3 meses antes de cuando cayo como cuenta hacker
WHERE 1=1
AND p.pay_status_id = 'approved'
and p.tpv_flag = 1
and h.user_id = 1181462578 --Poner user a evaluar
group by 1,2,3,4,5,6,7,8,9,10