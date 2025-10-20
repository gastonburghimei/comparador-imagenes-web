------ STEP 7
-- Segmentación de ATO por Orígen

BEGIN

-- Creo una tabla con todos los cust_ids que sufrieron un ATO/DT, con información relevante (fecha de inicio y fin del ATO/DT, y amt aprobado en distintos flujos)

CREATE TEMP TABLE fechas_inicio_fin as
    SELECT
        ato.cust_id,
        -- MAX(ato.usuario_patot) usuario_patot,
        MAX(ato.kyc_entity_type) kyc_entity_type,
        MAX(ato.kyc_knowledge_level) kyc_knowledge_level,
        MAX(ato.cust_segment_cross) cust_segment_cross,
        MAX(ato.cust_sub_segment_cross) cust_sub_segment_cross,
        MAX(ato.tipo_robo) tipo_robo,
        MAX(ato.site_id) site_id,
        MIN(COALESCE(ctx.op_creation_date, CAST(ato.op_datetime AS TIMESTAMP))) fecha_inicio,
        MAX(COALESCE(ctx.op_creation_date, CAST(ato.op_datetime AS TIMESTAMP))) fecha_fin,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'MC' then ato.op_amt else 0 end) amt_appr_mc,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'MI' and COALESCE(ato.pay_operation_type_id,'') != 'account_fund' then ato.op_amt else 0 end) amt_appr_mi,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'MI' and COALESCE(ato.pay_operation_type_id,'') = 'account_fund' then ato.op_amt else 0 end) amt_appr_mi_fondeo,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'PI' and COALESCE(ato.pay_operation_type_id,'') != 'account_fund' then ato.op_amt else 0 end) amt_appr_pi,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'PI' and COALESCE(ato.pay_operation_type_id,'') = 'account_fund' and COALESCE(ato.payment_method,'') = 'debin_transfer' then ato.op_amt else 0 end) amt_appr_pi_fondeo_debin,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'PI' and COALESCE(ato.pay_operation_type_id,'') = 'account_fund' and COALESCE(ato.payment_method,'') != 'debin_transfer' then ato.op_amt else 0 end) amt_appr_pi_fondeo_otros,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'MT' and COALESCE(ato.config_id_ctx,'') LIKE '%CRS%' then ato.op_amt else 0 end) amt_appr_crypto_venta,
        SUM(case when ato.status_id = 'A' and ato.flow_type = 'MT' and COALESCE(ato.config_id_ctx,'') LIKE '%CRB%' then ato.op_amt else 0 end) amt_appr_crypto_compra,
        SUM(case when ato.status_id = 'A' and ato.flow_type NOT IN ('MI', 'MC', 'PI') and COALESCE(ato.pay_operation_type_id,'') = 'account_fund' then ato.op_amt else 0 end) amt_appr_fondeo_otros,
        SUM(case when ato.status_id = 'A' and ato.flow_type IN ('MT', 'MO', 'PO') then ato.op_amt else 0 end) amt_appr_salida, -- 03/01
        -- SUM(case when ato.status_id IN ('A', 'D') and ato.flow_type NOT IN ('MI', 'MC', 'PI', 'ticket-atm') and COALESCE(ato.pay_operation_type_id,'') != 'account_fund' and COALESCE(ato.pay_operation_type_id,'') NOT LIKE 'money_exchange%' then ato.op_amt else 0 end) amt_appr_salida,
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato 
    LEFT JOIN (select execution_id,
       created_date,
       op_creation_date,
       prediction_provider_id from `warehouse-cross-pf.scoring_po.context`
 UNION ALL
  select execution_id,
       created_date,
       op_creation_date,
       prediction_provider_id from `warehouse-cross-pf.scoring_pay.context`) ctx 
    on ato.scoring_id = ctx.execution_id -- CAMBIO DE LA SCORING: ctx.scoring_id 
    and cast(ctx.created_date as date) > current_date - 400
    WHERE
        COALESCE(cast(ctx.created_date as date), current_date) >=  DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 20 MONTH), INTERVAL (EXTRACT(DAY FROM CURRENT_DATE())-1) DAY)
        and (ato.marca_ato > 0 or ato.marca_ato IS NULL)  -- contramarca (saco los -1, -2)
        and COALESCE(ctx.prediction_provider_id, 'pd') NOT IN ('business_rules', 'business_rules_mt')
        and ato.contramarca = 0 -- Agregue 2/01
    GROUP BY 1
;

-- Para todos los users que hayan sufrido un ATO/DT, sumo la cantidad que pidieron de créditos, y lo que se monetizó de este crédito
-- Ojo que acá los créditos marcados con ATO/DT siguen un criterio de haber sido pedidos en +-24hs del ATO/DT

CREATE TEMP TABLE creditos_por_user as
    SELECT
        fec.cust_id,
        cre.crd_credit_type,
        SUM(case when cre.originacion = 1 and cre.incoming = 1 then cre.crd_credit_amount_usd else 0 end) amt_appr_fondeo_cred,
        SUM(case when cre.originacion != 1 and cre.incoming = 1 then cre.crd_credit_amount_usd else 0 end) amt_rej_fondeo_cred,
        SUM(COALESCE(cre.monetizado, 0)) amt_appr_monetizado_cred,
    FROM fechas_inicio_fin fec LEFT JOIN `meli-sbox.PFCREDITS.scoring_origin_credits_v2` cre on fec.cust_id = cre.cus_cust_id_borrower
    WHERE COALESCE(cre.ato, 0) > 0 or COALESCE(cre.dt, 0) > 0
    GROUP BY 1, 2
;

-- Para todos los users que hayan sufrido un ATO/DT, sumo la cantidad que pidieron de adelantos de dinero, y lo que se monetizó de esos adelantos
-- Ojo que acá los adelantos marcados con ATO/DT siguen un criterio de haber sido pedidos en +-24hs del ATO/DT

-- Se rompió la tabla de adelantos
/*CREATE TEMP TABLE adelantos_por_user as
    SELECT
        mia.cus_cust_id cust_id,
        SUM(mia.total_inusd) amt_appr_fondeo_mia,
        SUM(COALESCE(fra.monetizado, 0)) amt_appr_monetizado_mia,
    FROM `meli-bi-data.WHOWNER.BT_MP_MIA` mia LEFT JOIN `meli-bi-data.WHOWNER.BT_MP_MIA_WITHDRAWAL` mwit on mwit.money_advance_id = mia.money_advanced_id
	    LEFT JOIN `meli-bi-data.WHOWNER.BT_MP_WITHDRAWALS` wit on wit.wit_withdrawal_id = mwit.wit_withdrawal_id
	        LEFT JOIN `meli-bi-data.SBOX_PF_DA.fraude_mia_ato` fra on fra.money_advanced_id = mia.money_advanced_id
	WHERE
        mia.creation_date >=  DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 20 MONTH), INTERVAL (EXTRACT(DAY FROM CURRENT_DATE())-1) DAY)
        and (COALESCE(fra.ato, 0) > 0 or COALESCE(fra.dt, 0) > 0)
        and mia.status = 'DONE'
    GROUP BY 1
;*/

-- Nueva tabla de adelantos
CREATE TEMP TABLE adelantos_por_user as
    SELECT
        mia.cus_cust_id as cust_id,
        SUM(mia.total_inusd) as amt_appr_fondeo_mia,
        SUM(mia.monetizado) as amt_appr_monetizado_mia,
    FROM `meli-sbox.PFCREDITS.scoring_origin_mia_v2` mia
	WHERE mia.mia_creation_date >=  DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 20 MONTH), INTERVAL (EXTRACT(DAY FROM CURRENT_DATE())-1) DAY)
	      and mia.status = 'DONE'
    GROUP BY 1
;

-- En base a las tablas previas, creo una tabla que tenga la información del orígen de lo monetizado (V1)

CREATE TEMP TABLE ato_origen_por_cust as
    SELECT DISTINCT
        fec.*,
        cre.* EXCEPT(cust_id),
        mia.* EXCEPT(cust_id),
        LEAST(amt_appr_mi_fondeo, amt_appr_salida) amt_appr_mi_fondeo_monetizado,
        LEAST(amt_appr_pi_fondeo_debin, amt_appr_salida) amt_appr_pi_monetizado_debin,
        LEAST(amt_appr_pi_fondeo_otros, amt_appr_salida) amt_appr_pi_monetizado_otros,
        GREATEST(amt_appr_crypto_venta - amt_appr_crypto_compra, 0) amt_appr_crypto_fondeo,
        LEAST(GREATEST(amt_appr_crypto_venta - amt_appr_crypto_compra, 0), amt_appr_salida) amt_appr_crypto_monetizado,
        LEAST(amt_appr_fondeo_otros, amt_appr_salida) amt_appr_fondeo_otros_monetizado,
        GREATEST(
            amt_appr_salida
            - COALESCE(amt_appr_monetizado_cred, 0)
            - COALESCE(amt_appr_monetizado_mia, 0)
            - LEAST(amt_appr_mi_fondeo, amt_appr_salida)
            - LEAST(amt_appr_pi_fondeo_debin, amt_appr_salida)
            - LEAST(amt_appr_pi_fondeo_otros, amt_appr_salida)
            - LEAST(GREATEST(amt_appr_crypto_venta - amt_appr_crypto_compra, 0), amt_appr_salida)
            - LEAST(amt_appr_fondeo_otros, amt_appr_salida)
        , 0) as amt_appr_monetizado_am
    FROM fechas_inicio_fin fec
        LEFT JOIN creditos_por_user cre on fec.cust_id = cre.cust_id
            LEFT JOIN adelantos_por_user mia on fec.cust_id = mia.cust_id
;

-- Por último, esa misma tabla anterior la transpongo para que sea más sencillo de trabajar en Tableau

DROP TABLE IF EXISTS `meli-bi-data.SBOX_PFFINTECHATO.ato_origen`;
CREATE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_origen` as (
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_mc' as origen,
        amt_appr_mc as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_mi' as origen,
        amt_appr_mi as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_mi_fondeo' as origen,
        amt_appr_mi_fondeo as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_pi' as origen,
        amt_appr_pi as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_pi_fondeo_debin' as origen,
        amt_appr_pi_fondeo_debin as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_pi_fondeo_otros' as origen,
        amt_appr_pi_fondeo_otros as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_fondeo_otros' as origen,
        amt_appr_fondeo_otros as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_salida' as origen,
        amt_appr_salida as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_mi_fondeo_monetizado' as origen,
        amt_appr_mi_fondeo_monetizado as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_pi_monetizado_debin' as origen,
        amt_appr_pi_monetizado_debin as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_pi_monetizado_otros' as origen,
        amt_appr_pi_monetizado_otros as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_crypto_fondeo' as origen,
        amt_appr_crypto_fondeo as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_crypto_monetizado' as origen,
        amt_appr_crypto_monetizado as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_fondeo_otros_monetizado' as origen,
        amt_appr_fondeo_otros_monetizado as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        case when crd_credit_type = 'MERCHANT' then 'amt_appr_fondeo_cred_merchant'
            when crd_credit_type = 'CONSUMER' then 'amt_appr_fondeo_cred_consumer' end as origen,
        amt_appr_fondeo_cred as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        case when crd_credit_type = 'MERCHANT' then 'amt_rej_fondeo_cred_merchant'
            when crd_credit_type = 'CONSUMER' then 'amt_rej_fondeo_cred_consumer' end as origen,
        amt_rej_fondeo_cred as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        case when crd_credit_type = 'MERCHANT' then 'amt_appr_monetizado_cred_merchant'
            when crd_credit_type = 'CONSUMER' then 'amt_appr_monetizado_cred_consumer' end as origen,
        amt_appr_monetizado_cred as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_fondeo_mia' as origen,
        amt_appr_fondeo_mia as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_monetizado_mia' as origen,
        amt_appr_monetizado_mia as amt
    FROM ato_origen_por_cust ato
    UNION ALL
    SELECT
        cust_id,
        -- usuario_patot,
        kyc_entity_type,
        kyc_knowledge_level,
        cust_segment_cross,
        cust_sub_segment_cross,
        tipo_robo,
        site_id,
        fecha_inicio,
        fecha_fin,
        'amt_appr_monetizado_am' as origen,
        amt_appr_monetizado_am as amt
    FROM ato_origen_por_cust ato
)
;

END;

----- STEP 8
BEGIN
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.perfil_users_tmp` as (
SELECT distinct cast (GCA_CUST_ID as NUMERIC) as GCA_CUST_ID,
min (casuistica_ato) as casuistica,
min (modalidad_ato) as modalidad
FROM `meli-bi-data.SBOX_PFFINTECHATO.perfilado_usuarios_ATO`
GROUP BY 1
);
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.origen_dto` as (
SELECT distinct cast (CUST_ID as NUMERIC) as GCA_CUST_ID, 
min (tipo_dto) as casuistica_dto,
min (tipo_dto) as modalidad_dto
FROM `meli-bi-data.SBOX_PFFINTECHATO.tipo_dt`
GROUP BY 1
);
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.prueba_perfiles` as (
SELECT ato.*EXCEPT(casuistica, modalidad),
case when ato.tipo_robo = 'DT' then c.casuistica_dto
else b.casuistica
end as casuistica,
case when ato.tipo_robo = 'DT' then c.modalidad_dto
else b.modalidad
end as modalidad
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.perfil_users_tmp` b
on ato.cust_id = b.GCA_CUST_ID
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.origen_dto` c
on ato.cust_id = c.GCA_CUST_ID
);

-- ACTUALIZAR LA VARIABLE CASUISTICA A LA BQ CORRECTAMENTE
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
SET bq.casuistica = p.casuistica 
FROM `meli-bi-data.SBOX_PFFINTECHATO.prueba_perfiles` p
WHERE bq.operation_id = cast(p.operation_id as string) -- cambie a string por lo de tcmp
;

-- ACTUALIZAR LA VARIABLE MODALIDAD A LA BQ CORRECTAMENTE
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
SET bq.modalidad = p.modalidad
FROM `meli-bi-data.SBOX_PFFINTECHATO.prueba_perfiles` p
WHERE bq.operation_id = cast(p.operation_id as string) -- cambie a string por lo de tcmp
;

-- ACTUALIZAR CASUISTICA Y MODALIDAD DEEPFAKE PARA USUARIOS AFECTADOS
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
SET casuistica = 'deepfake'
WHERE cust_id IN (36401950, 130143317, 1024296786, 159971135, 67143382, 42484193, 741390374, 116190522, 6311616, 92593322, 183511902,
                  8091300, 68121547, 67823782, 204146483, 34520518, 2444107423, 2447420744, 99635156, 359144857, 79107210, 181998557,
                  391882199, 15965371, 1773355510, 675181325, 40117920, 6241477, 120575323, 47580834, 696034096, 187208006, 87661506,
                  303872432, 571086533, 2254194, 202482139, 392058987, 199236254, 217612042, 1197607, 149640830, 8047646, 803958987,
                  432242380, 755521542, 435505123, 827142056, 53590123, 225118659, 327963748, 97090131, 1088135279, 94525641, 1637140625,
                  311454265, 484739736, 2084139892, 15812448, 258451018, 46784, 301718914, 2004269667, 105046514, 87213422, 180907672,
                  207590129, 375248484, 219498475, 142560874, 57114589, 87143417, 82010982, 191190937, 20700285, 499519080, 206752784,
                  283421986, 1182059123, 88331821, 157839717, 88342972, 582362520, 236359295);

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
SET modalidad = 'deepfake'
WHERE cust_id IN (36401950, 130143317, 1024296786, 159971135, 67143382, 42484193, 741390374, 116190522, 6311616, 92593322, 183511902,
                  8091300, 68121547, 67823782, 204146483, 34520518, 2444107423, 2447420744, 99635156, 359144857, 79107210, 181998557,
                  391882199, 15965371, 1773355510, 675181325, 40117920, 6241477, 120575323, 47580834, 696034096, 187208006, 87661506,
                  303872432, 571086533, 2254194, 202482139, 392058987, 199236254, 217612042, 1197607, 149640830, 8047646, 803958987,
                  432242380, 755521542, 435505123, 827142056, 53590123, 225118659, 327963748, 97090131, 1088135279, 94525641, 1637140625,
                  311454265, 484739736, 2084139892, 15812448, 258451018, 46784, 301718914, 2004269667, 105046514, 87213422, 180907672,
                  207590129, 375248484, 219498475, 142560874, 57114589, 87143417, 82010982, 191190937, 20700285, 499519080, 206752784,
                  283421986, 1182059123, 88331821, 157839717, 88342972, 582362520, 236359295);

END

------ STEP 9
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ratio_tpv` as (
SELECT SITE,
PRODUCT_TYPE,
  CASE
    WHEN PRODUCT_TYPE IN ('DIRECT', 'CARRITO') THEN 'MARKETPLACE'
    WHEN PRODUCT_TYPE IN ('API', 'COW', 'LINK', 'WALLET BUTTON',
                          'WALLET CONNECT', 'PREAPPROVAL', 
                          'LOYALTY', 'SUBSCRIPTION'
                          ) THEN 'ONLINE PAYMENTS'
    WHEN PRODUCT_TYPE IN ('MONEY TRANSFER', 'QR', 'QR-INTEROP',
                          'BOLETO', 'ACCOUNT FUND'
                         ) THEN 'WALLET'
    ELSE 'OTROS'
  END AS VERTICAL,
FECHA,
SUM(TPV) AS TPV,
USER_AGE,
RANGO_ASP
FROM `meli-bi-data.SBOX_PF_PAY_METRICS.APPROVAL_METRICS` app
WHERE 1=1
/*AND FECHA >= '2023-01-01'*/
AND app.PAY_TRY_LAST = 1
GROUP BY ALL
ORDER BY 1,4
);

