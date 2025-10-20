--PRIMERO CREO LA TABLA DE FACEOFF PASANDO LAS FECHAS A COMO ESTA EN LA ATO_BQ

CREATE OR REPLACE TABLE `meli-bi-data.EXPLOTACION.fraud_ato_faceoff_prod_fecha_ok` AS (
  SELECT case_id,
  user_id,
  user_type,
  site_id,
  channel_id,
  max_balance,
  credit_line,
  monthly_tpv,
  wallet_type,
  wallet_priority,
  faceoff_status,
  faceoff_error,
  faceoff_reason,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S',
    TIMESTAMP(REGEXP_REPLACE(NULLIF(faceoff_date, ''), r'\.[0-9]{1,9}Z$', ''))) AS faceoff_date_formateada,
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', creation_date) AS creation_date_formateada
  FROM `meli-bi-data.EXPLOTACION.fraud_ato_faceoff_prod`
  WHERE 1=1
  AND faceoff_date IS NOT NULL 
  AND faceoff_date <> ''
)
;
SELECT *
FROM `meli-bi-data.EXPLOTACION.fraud_ato_faceoff_prod_fecha_ok`
--ESTO TRAE 9441 CASOS (15 de mayo), SE LIMPIAN UN POCO.

--AHORA CREO LA TABLA DE FACEOFF PARA CLASIFICAR SI EL ATO ES PRE O POST
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.faceoff_ato_ant_post` AS (
  SELECT face.*,
  bq.marca_ato,
  bq.op_amt,
  bq.op_datetime,
  bq.status_id,
  bq.casuistica,
  bq.modalidad,
  CASE 
    WHEN DATE(TIMESTAMP(face.faceoff_date_formateada)) < DATE(bq.op_datetime) THEN 'POST'
    WHEN DATE(TIMESTAMP(face.faceoff_date_formateada)) > DATE(bq.op_datetime) THEN 'PRE'
    else 'NO ATO'
  END as estado
  FROM `meli-bi-data.EXPLOTACION.fraud_ato_faceoff_prod_fecha_ok` face
    LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
      ON face.user_id = bq.cust_id
  WHERE 1=1
)
;
select *
from `meli-bi-data.SBOX_PFFINTECHATO.faceoff_ato_ant_post`
--ESTO ME TRAE 10043 CASOS AL 15 DE MAYO