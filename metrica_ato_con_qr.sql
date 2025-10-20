with operacion_marca as (
  select
    format_timestamp('%m-%Y', bq.op_datetime) as month_year,
    bq.operation_id,
    bq.cust_id,
    bq.status_id,
    bq.op_amt,
    bq.action_field_value,
    bq.flow_type,
    bq.vertical,
    bq.producto,
    bq.payment_method,
    bq.pay_operation_type_id,
    bq.marca_ato,
    bq.op_datetime,
    bq.tipo_robo,
    bq.tier_ato,
    bq.device_id,
    bq.device_creation_date,
    bq.casuistica
  from  `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
where 1=1
  and site_id = 'MLB'
  and op_datetime >= '2025-01-01'
  and tipo_robo = 'ATO'
  and contramarca = 0
)

SELECT
  det.*,
  ori.user_id,
  ARRAY_TO_STRING(ori.completed_elements, ', ') AS completed_elements_new,
  SUBSTRING(ori.raw, INSTR(ori.raw, 'detached_id":"') + LENGTH('detached_id":"'), 36) AS detached_id,
  ori.platform_id,
  ori.device_id,
  datetime,
  DATE_DIFF(det.op_datetime,ori.datetime, hour) as antiguedad_detached_horas,
  DATE_DIFF(det.op_datetime,ori.datetime, day) as antiguedad_detached_dias,
from operacion_marca det 
  left join `authevents-6ikh1q8l2t2-furyid.login.authentication_challenges` ori
    on det.device_id = ori.device_id 
    and det.cust_id = ori.user_id
WHERE 1=1
  and ori.datetime >= '2024-06-01' 
  and ori.datetime <= det.op_datetime
  AND ori.code = 'grant_session'