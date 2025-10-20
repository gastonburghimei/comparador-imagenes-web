CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_infracciones_historico` AS (
SELECT
      res.user_id,
      res.sentence_id,
      res.infraction_type,
      'alta' AS certainty,
      "AUTO" AS detection_type,
      'rehab_ato_infracciones_historico' AS name,
      'rehab_ato_infracciones_historico' AS reason,
      'PF' AS reporting_area,
      'ATO_rehab_infracciones_historico' AS detector
FROM `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
  LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.infracciones_ato_rehab_historico` his
   ON res.user_id = his.user_id
WHERE 1=1
  AND res.infraction_type = 'ATO'
  AND res.sentence_last_status = 'ACTIVE'
  AND his.etapa_rehab = 1
)
;

--Query para monitorear si hubo casos de ato
select
    user_id,
    sentence_id,
    infraction_type,
    bq.marca_ato,
    bq.op_dt,
    bq.op_amt,
    bq.tipo_robo
from `meli-bi-data.SBOX_PFFINTECHATO.ato_infracciones_historico`
  left join `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
    on user_id = cust_id
where 1=1
  --and bq.marca_ato = 1
  --and bq.contramarca = 0
  --and bq.status_id = 'A'
  and bq.op_dt >= '2025-03-13' --fecha en la que rehabilitamos esas cuentas
;