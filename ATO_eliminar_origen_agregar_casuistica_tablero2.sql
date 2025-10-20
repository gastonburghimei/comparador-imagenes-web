--STEP 7, LINEA 405

CREATE TEMP TABLE pre_mashup AS (
SELECT ato.cust_id, ato.site_id, ato.fecha_inicio, ato.fecha_fin, ato.tipo_robo,
ato.kyc_knowledge_level, ato.cust_segment_cross, ato.cust_sub_segment_cross,
--case 
  --when cook.marcaje_cookies is not null then 'Cookies'
  --when mal.cust_id is not null then 'Malware'
  --else ato.origen
--end as origen_final,
ato.ato_prueba,
ato.ato_monetizado_total as monetizado_total,
ato.ato_monetizado_mi as mi_monetizado,
-- credits
cr.cust_id as cr_cust_id,
cr.sit_site_id as cr_site_id,
cr.created_date as cr_created_date,
cr.month as cr_month, 
cr.crd_credit_type as cr_crd_credit_type,
cr.tipo_robo as cr_tipo_robo,
cr.credit_type as cr_credit_type,
case 
  when credit_type = 'Consumer Credits' then 0
  else 1
end as flag_credits,
cr.amt_monetizado as credits_monetizado,
case 
  when mal.cust_id is not null then ato.ato_monetizado_total
  else 0
end as monetizado_malware,
cook.monetizado_cookies
-- ck.amt_aprobada
FROM ato_total ato
FULL OUTER JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_credits_total` cr
on ato.cust_id = cr.cust_id
FULL OUTER JOIN malware mal
on ato.cust_id = mal.cust_id
FULL OUTER JOIN cookies_final cook
on ato.cust_id = cook.cust_id
/*LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_cookies_appr` ck
on ato.cust_id = cook.cust_id*/
);