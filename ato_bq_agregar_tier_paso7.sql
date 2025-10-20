-- PASO 7 DE LA ATO_BQ

-- Se quiere agregar TIER en la hoja de ATO CREDITS y la fuente que alimenta esa hoja
-- es la de ato_credits_total que esta en el paso 7 de la ato_bq.

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_credits_total_GB` AS (
SELECT cr.*,
seg.CUST_SEGMENT_CROSS as cust_segment_cross,
seg.CUST_SUB_SEGMENT_CROSS as cust_sub_segment_cross,
prioridad_final,
  case
    when ti.tier = 1 then 'Tier 1'
    when ti.tier = 2 then 'Tier 2'
    else null
  end as Tier_ATO
FROM pre_ato_total_credits cr
LEFT JOIN (
      SELECT
          CUS_CUST_ID,
          CUST_SEGMENT_CROSS,
          CUST_SUB_SEGMENT_CROSS
      FROM `meli-bi-data.WHOWNER.LK_MP_SEGMENTATION_SELLERS`
      QUALIFY 1 = row_number () OVER (PARTITION BY CUS_CUST_ID ORDER BY TIM_MONTH DESC)
      ) seg
ON cr.cust_id = seg.cus_cust_id
LEFT JOIN regla_prioridad prio
ON cr.cust_id = cast (prio.GCA_CUST_ID as numeric)
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.tiers_ato` ti 
ON cr.cust_id = ti.cust_id
);

-- Me parecia llamativo al principio que solo pudiera ser agregar la variable 
-- ahi llamando a la tabla que trae la informacion pero con las 3 que estan en el
-- select pasa lo mismo, no son creadas antes. Y tiene sentido porque esa informacion
-- que traigo para los tier, cuando lo tiro en big query me trae correcta la info.