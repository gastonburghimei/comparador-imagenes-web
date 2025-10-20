--- STEP 6 DIO ERROR ACA

BEGIN

-- Merchant Credits

CREATE TEMP TABLE merchant AS (
SELECT cus_cust_id_borrower,
sit_site_id,
min(CAST (crd_credit_date_created_id AS DATE)) as created_date,
extract (month from min(crd_credit_date_created_id)) as month,
crd_credit_type,
crd_credit_subtype,
max(ato) as ato,
max(dt) as dto,
sum(monetizado) as monetizado_merchant
FROM `meli-sbox.PFCREDITS.scoring_origin_credits_v2`
where crd_credit_type='MERCHANT' 
and crd_credit_date_created_id >= '2022-01-01'
and (ato = 1 or dt = 1)
group by 1,2,5,6
order by 1 -- 1.132 cust unicos
);

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_merchant` AS (
SELECT cus_cust_id_borrower,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
case
  when (ato = 1 and dto = 0) then 'ATO'
  when (ato = 0 and dto = 1) then 'DT'
else 'ATO'
end as marcaje_merchant,
monetizado_merchant
FROM merchant
ORDER BY 1
);


-- Personal Loans

CREATE TEMP TABLE personal_loans AS (
SELECT cus_cust_id_borrower,
sit_site_id,
min(CAST (crd_credit_date_created_id AS DATE)) as created_date,
extract (month from min(crd_credit_date_created_id)) as month,
crd_credit_type,
crd_credit_subtype,
max(ato) as ato,
max(dt) as dto,
sum(monetizado) as monetizado_plo
FROM `meli-sbox.PFCREDITS.scoring_origin_credits_v2`
where crd_credit_type='CONSUMER' 
and crd_credit_date_created_id >= '2022-01-01'
and (ato = 1 or dt = 1)
group by 1,2,5,6
order by 1 -- 3.278 cust unicos
);

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_personal_loans` AS (
SELECT cus_cust_id_borrower,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
case
  when (ato = 1 and dto = 0) then 'ATO'
  when (ato = 0 and dto = 1) then 'DT'
else 'ATO'
end as marcaje_plo,
monetizado_plo
FROM personal_loans
ORDER BY 1
);


-- Consumer Credits

CREATE TEMP TABLE consumer_credits AS (
SELECT cus_cust_id_buy, 
sit_site_id, 
min(CAST (created_date AS DATE)) as created_date,
extract (month from min(created_date)) as month,
crd_credit_type,
crd_credit_subtype,
max(ato) as ato,
max(dt) as dto,
sum (op_dol_amount) as monetizado_consumer
FROM `meli-sbox.PFCREDITS.scoring_origin_consumer_credits_v2` 
where pcc_status = 'A'
and created_date >= '2022-01-01'
and (ato = 1 or dt = 1)
group by 1,2,5,6
order by 1 -- 4.790 custs unicos
);

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_consumer` AS (
SELECT cus_cust_id_buy,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
case
  when (ato = 1 and dto = 0) then 'ATO'
  when (ato = 0 and dto = 1) then 'DT'
else 'ATO'
end as marcaje_consumer,
monetizado_consumer
FROM consumer_credits
ORDER BY 1
);


-- MIA

CREATE TEMP TABLE mia AS (
SELECT
cus_cust_id,
sit_site_id,
min(CAST (mia_creation_date AS DATE)) as created_date,
extract (month from min(mia_creation_date)) as month,
'MIA' as crd_credit_type,
'MIA' as crd_credit_subtype,
max(ato) as ato,
max(dt) as dto,
sum (monetizado) as monetizado_mia
FROM `meli-sbox.PFCREDITS.scoring_origin_mia_v2` 
where status = 'DONE'
and mia_creation_date >= '2022-01-01'
and (ato = 1 or dt = 1)
GROUP BY 1,2
ORDER BY 1 -- 1.187 registros unicos
);

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_mia` AS (
SELECT cus_cust_id,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
case
  when (ato = 1 and dto = 0) then 'ATO'
  when (ato = 0 and dto = 1) then 'DT'
else 'ATO'
end as marcaje_mia,
monetizado_mia
FROM mia
ORDER BY 1
);

-- Fin parte Credits
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_merchant`
SET monetizado_merchant = 0 where monetizado_merchant is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_personal_loans`
SET monetizado_plo = 0 where monetizado_plo is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_consumer`
SET monetizado_consumer = 0 where monetizado_consumer is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_mia`
SET monetizado_mia = 0 where monetizado_mia is null
;

-- Para el origen del ATO primero

CREATE TEMP TABLE prioridad as (
  SELECT GCA_CUST_ID,
      MIN (pame.ORIGEN_ATO) as prioridad_min,
      MAX (pame.ORIGEN_ATO) as prioridad_max,
      MIN (fecha_apertura_caso) as fecha_minima, -- fecha minima para el conocimiento del evento
      MIN (red_social) as red_social_min,
      MAX (red_social) as red_social_max
  FROM `meli-bi-data.SBOX_PF_PAY_METRICS.CONSULTAS_ATO` pame
  GROUP BY 1
  ORDER BY 1
);

CREATE TEMP TABLE regla_prioridad as (
  SELECT prioridad.*,
      case 
        when prioridad_min is null then prioridad_max
        when prioridad_min is not null then prioridad_min   
        else null
      end as prioridad_final,
      case 
        when red_social_min is null then red_social_max
        when red_social_min is not null then red_social_min   
        else null
      end as red_social_final
  FROM prioridad
);

-- 

-- Tabla final de Credits

CREATE TEMP TABLE pre_ato_total_credits AS (
SELECT
cus_cust_id_borrower as cust_id,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
marcaje_merchant as tipo_robo,
'Merchant Credits' as credit_type,
monetizado_merchant as amt_monetizado
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_merchant`

UNION ALL
SELECT
cus_cust_id_borrower as cust_id,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
marcaje_plo as tipo_robo,
'Personal Loans' as credit_type,
monetizado_plo as amt_monetizado
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_personal_loans`

UNION ALL
SELECT
cus_cust_id_buy as cust_id,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
marcaje_consumer as tipo_robo,
'Consumer Credits' as credit_type,
monetizado_consumer as amt_monetizado
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_consumer`

UNION ALL
SELECT 
cus_cust_id as cust_id,
sit_site_id,
created_date,
month, 
crd_credit_type,
crd_credit_subtype,
marcaje_mia as tipo_robo,
'MIA' as credit_type,
monetizado_mia as amt_monetizado
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_mia`
);


CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_credits_total` AS (
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
ON cr.cust_id = cast (prio.GCA_CUST_ID as STRING) -- CAMBIE A STRING POR LO DE TCMP EN PASO 1
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.tiers_ato` ti 
ON cr.cust_id = ti.cust_id
);


-- Malware
CREATE TEMP TABLE malware AS (
SELECT
    ato.cust_id,
    MAX(site_id) as site_id,
    MIN(op_dt) as fecha_inicio,
    MAX(op_dt) as fecha_fin,
    MAX(tipo_robo) as tipo_robo,
    MAX(ato.kyc_knowledge_level) as kyc_knowledge_level,
    MAX(ato.cust_segment_cross) as cust_segment_cross,
    MAX(ato.cust_sub_segment_cross) as cust_sub_segment_cross,
    MAX(prioridad_final) as origen,
    case when com.cust_id IS NULL then 1 else 0 end as manual
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
LEFT JOIN (
        SELECT DISTINCT CAST(GCA_CUST_ID AS STRING) cust_id -- cambie a string por el paso 1 con lo de tcmp
        FROM `meli-bi-data.WHOWNER.BT_FRD_GENERAL_CASES_MANUALREW_EXP`
        WHERE
            (GCA_COMMENT LIKE ('%[ato por malware] perfil infostealer%') or GCA_COMMENT LIKE ('%[ato por malware] perfil troyanos bancarios%') or GCA_COMMENT LIKE ('%[ato por malware] perfil malware%'))
            and GCA_DATE_CREATED >= '2021-10-01'
    ) com on ato.cust_id = com.cust_id
WHERE
    (com.cust_id IS NOT NULL or ato.cust_id IN (348902054, 315187666, 300707556, 325099142, 394121986, 579727803, 525134798, 661374596, 128717749, 319304532, 434127664, 261084196, 706794464, 189537188, 49305554, 341568683, 787955574, 401550684, 553959235, 328843673, 226842428, 140051347, 284280500, 322455938, 555095654, 599622251, 599987862, 299194917, 349255674, 578803339, 568457550, 424885075, 480068206, 430276288, 90048407, 553857896, 177928932, 178551941, 307812685, 227596415, 364700292, 399283088, 545641310, 4858730, 316925207, 126741421, 735642778, 46900590, 746582173, 426016713, 68362805, 152679971, 135387535, 180666989, 397087258, 56038708, 75800668, 158310950, 436454942, 224580075, 312173502, 582751093, 463484361, 782255575, 236090276, 79447355, 96374928, 756736098, 7593624, 134751150, 385181447, 203427742, 194762297, 213828435, 271414795))
    and op_dt >= '2021-10-01'
    and ato.cust_id NOT IN (621548524)
    -- and contramarca = 0
    -- and status_id IN ('A', 'D')
GROUP BY 1,10
ORDER BY 1
);



-- Robo de cookies

CREATE TEMP TABLE cookies AS (
SELECT cust_id, 
max (site_id) as site_id,
min (flow_type) as flow_type_min, 
max (flow_type) as flow_type_max,
max (Ato_bq) as ato_bq,
max (dt_bq) as dt_bq,
min(op_dt) as op_dt_min,
extract (month from min(op_dt)) as month,
sum(op_amt) as monetizado_cookies
FROM `meli-bi-data.SBOX_PF_DA.ato_anomalias_enero_julio`
WHERE ataque = 1 and contramarca = 0
and op_dt >= '2023-01-01'
group by 1
order by 1
);


CREATE TEMP TABLE cookies_final AS (
SELECT cust_id, site_id, flow_type_min, flow_type_max, op_dt_min, month,
case 
  when (Ato_bq = 1 and dt_bq = 0) then 'ATO'
  when (Ato_bq = 0 and dt_bq = 1) then 'DT'
else 'Both'
end as marcaje_cookies,
monetizado_cookies
FROM cookies
ORDER BY 1
);

-- Lo aprobado segun ato_bq
/*CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_cookies_appr` AS(
SELECT cook.*,
 sum (case when status_id = 'A' and contramarca = 0 then op_amt
      else 0 end) as amt_aprobada,
 sum (case when status_id = 'C' and contramarca = 0 then op_amt
      else 0 end) as amt_cancelada,
 sum (case when status_id IN ( 'I' , 'R') and contramarca = 0 then op_amt
      else 0 end) as amt_rechazada,
 sum (case when status_id = 'P' and contramarca = 0 then op_amt
      else 0 end) as amt_pending,
 sum (case when status_id = 'D' and contramarca = 0 then op_amt
      else 0 end) as amt_refunded
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_cookies` cook
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
 on cook.cust_id = ato.cust_id
GROUP BY 1,2,3,4,5,6,7,8
);
*/

-- El ATO Total
CREATE TEMP TABLE ato_total as (
    SELECT 
    cust_id,
    MAX(site_id) as site_id,
    MIN(op_dt) as fecha_inicio,
    MAX(op_dt) as fecha_fin,
    MAX(tipo_robo) as tipo_robo,
    sum (case 
            when flow_type IN ('MT','MO','PO','MC') and ato.contramarca = 0 and status_id = 'A' then op_amt
            else 0
        end
    ) as ato_monetizado_total,
    sum (case
            when flow_type IN ('MT','MO','PO') and ato.contramarca = 0 and status_id = 'A' then op_amt
            else 0
        end
    ) as ato_prueba,
    sum (case 
            when flow_type = 'MI' and ato.contramarca = 0 and status_id = 'A' then op_amt
            else 0
        end
    ) as ato_monetizado_mi,
    MAX(ato.kyc_knowledge_level) as kyc_knowledge_level,
    MAX(ato.cust_segment_cross) as cust_segment_cross,
    MAX(ato.cust_sub_segment_cross) as cust_sub_segment_cross,
    MAX(prioridad_final) as origen
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
    WHERE contramarca = 0
    GROUP BY 1
    ORDER BY 1
);


CREATE TEMP TABLE pre_mashup AS (
SELECT ato.cust_id, ato.site_id, ato.fecha_inicio, ato.fecha_fin, ato.tipo_robo,
ato.kyc_knowledge_level, ato.cust_segment_cross, ato.cust_sub_segment_cross,
 -- case 
   -- when cook.marcaje_cookies is not null then 'Cookies'
   -- when mal.cust_id is not null then 'Malware'
   -- else ato.origen
 -- end as origen_final,
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

/*CREATE TEMP TABLE mashup AS (
SELECT ato.cust_id, ato.site_id, ato.fecha_inicio, ato.fecha_fin, ato.tipo_robo,
ato.kyc_knowledge_level, ato.cust_segment_cross, ato.cust_sub_segment_cross,
case 
  when marcaje_cookies is not null then 'Cookies'
  when mal.cust_id is not null then 'Malware'
  else ato.origen
end as origen_final,
ato.ato_monetizado_total as monetizado_total, 
ato.ato_monetizado_mi as monetizado_mi,
monetizado_merchant,
monetizado_plo,
monetizado_consumer,
monetizado_mia,
monetizado_cookies,
case 
  when mal.cust_id is not null then ato.ato_monetizado_total
  else 0
end as monetizado_malware
FROM ato_total ato
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_merchant` mer
  on ato.cust_id = mer.cus_cust_id_borrower
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_personal_loans` plo
  on ato.cust_id = plo.cus_cust_id_borrower
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_consumer` con
  on ato.cust_id = con.cus_cust_id_buy
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_mia` mia
  on ato.cust_id = mia.cus_cust_id
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_malware` mal
  on ato.cust_id = mal.cust_id
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_cookies` cook
  on ato.cust_id = cook.cust_id
);
*/

UPDATE pre_mashup
SET monetizado_total = 0 where monetizado_total is null
;

UPDATE pre_mashup
SET mi_monetizado = 0 where mi_monetizado is null
;

UPDATE pre_mashup
SET monetizado_cookies = 0 where monetizado_cookies is null
;

/*UPDATE pre_mashup
SET amt_aprobada = 0 where amt_aprobada is null
;*/

UPDATE pre_mashup
SET monetizado_malware = 0 where monetizado_malware is null
;

UPDATE pre_mashup
SET credits_monetizado = 0 where credits_monetizado is null
;


CREATE OR REPLACE TABLE  `meli-bi-data.SBOX_PFFINTECHATO.mashup` AS (
SELECT pre.*,
/*case 
  when monetizado_total = 0 then 0
  when monetizado_total <> 0 then (monetizado_total - credits_monetizado) 
end as ato_monetizado_am*/
(monetizado_total - credits_monetizado) as ato_monetizado_am
FROM pre_mashup pre
);

-- Agregado el 21/02 por los negativos cuando am es 0 y fue todo por credits

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.mashup`
SET ato_monetizado_am = 0 where (monetizado_total = 0 and credits_monetizado <> 0)
;
-- Verlo mañana

/*
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma` AS (
SELECT cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
origen_final,
monetizado_total, 
monetizado_mi,
monetizado_cookies,
monetizado_malware,
monetizado_merchant,
monetizado_plo,
monetizado_consumer,
monetizado_mia,
(monetizado_merchant + monetizado_plo + monetizado_consumer + monetizado_mia) as ato_monetizado_credits,
(monetizado_total - monetizado_mi - (monetizado_merchant + monetizado_plo + monetizado_consumer + monetizado_mia) - monetizado_cookies - monetizado_malware) as ato_monetizado_am
FROM mashup
);


CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_final_paloma` AS (
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'mi_monetizado' as origen,
  monetizado_mi as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma` 

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'cookies_monetizado' as origen,
  monetizado_cookies as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma` 

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'malware_monetizado' as origen,
  monetizado_malware as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma` 

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'total_monetizado' as origen,
  monetizado_total as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma`

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'merchant_monetizado' as origen,
  monetizado_merchant as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma`

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'plo_monetizado' as origen,
  monetizado_plo as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma`

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'consumer_monetizado' as origen,
  monetizado_consumer as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma`

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'mia_monetizado' as origen,
  monetizado_mia as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma`

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'credits_total_monetizado' as origen,
  ato_monetizado_credits as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma`

UNION ALL
SELECT
  cust_id, site_id, fecha_inicio, fecha_fin, tipo_robo,
  kyc_knowledge_level, cust_segment_cross, cust_sub_segment_cross,
  origen_final,
  'am_monetizado' as origen,
  ato_monetizado_am as amt
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_origen_paloma`

);
*/

END;
