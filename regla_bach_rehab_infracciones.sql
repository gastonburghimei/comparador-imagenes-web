CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_infracciones_pasadas` AS (
SELECT
      res.user_id,
      res.sentence_id,
      res.infraction_type,
      'alta' AS certainty,
      "AUTO" AS detection_type,
      'rehab_ato_infracciones_pasadas' AS name,
      'rehab_ato_infracciones_pasadas' AS reason,
      'PF' AS reporting_area,
      'ATO_rehab_infracciones_pasadas' AS detector
FROM `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
  LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.infracciones_ato_rehab_etapas` inf
   ON res.user_id = inf.user
WHERE 1=1
  AND res.infraction_type = 'ATO'
  AND res.color_de_tarjeta = '2'
  AND res.sentence_last_status = 'ACTIVE'
  AND inf.etapa_rehab = 4
);