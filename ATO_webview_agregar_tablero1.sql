-- PASO 2 DE LA ATO_BQ
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.pyt_temp_tmp_GB`  as
(
    SELECT DISTINCT
        pyt.payout_id as operation_id
        ,pyt.cus_cust_id as cust_id
        ,pyt.cus_cust_id as receiver_id
        -- ,pyt.pyt_orgn_accnt_type --ted
        ,pyt.payout_type --atm
        ,case 
            when pyt.PAYOUT_STATUS = 'approved' then 'A'
            when pyt.PAYOUT_STATUS = 'charged_back' then 'A'
            when pyt.PAYOUT_STATUS = 'cancelled' then 'C'
            when pyt.PAYOUT_STATUS = 'in_process' then 'P'
            when pyt.PAYOUT_STATUS = 'authorized' then 'P'
            when pyt.PAYOUT_STATUS = 'refunded' then 'D'
            when pyt.PAYOUT_STATUS = 'rejected' and pyt.PYT_STATUS_DETAIL = 'high_risk' then 'I'
            when pyt.PAYOUT_STATUS = 'rejected' then 'R'
            else null
        end as status_id
        ,REGEXP_REPLACE(pyt.sit_site_id, ' ', '') as site_id 
        ,pyt.pyt_created_dt as op_dt
        ,pyt.total_payout_amt as lc_op_amt -- 20/10
        ,cur.CCO_TC_VALUE as tipo_cambio
        ,(pyt.total_payout_amt/cur.CCO_TC_VALUE) as op_amt
        ,sco.config_id as config_id
        ,sco.flow_type as flow_type
        ,case
            when sco.config_id = 'QRP' then 'PO-QRP'
            when sco.config_id = 'PIX' then 'PO-PIX'
            when sco.config_id = 'ATM' then 'PO-ATM'
            when sco.config_id = 'COT' then 'PO-COT'
            when sco.config_id = 'STD' then 'PO-STD'
            else sco.config_id
        end as vertical
        ,null as producto
        ,sco.points as points
        ,sco.profile_id as profile_id
        ,tko.action_field_value
        ,case when PRUEBA.ATO_CONTRAMARCA IS NOT NULL THEN -1 ELSE 1 END as marca_ato
        -- ,case when con.ato_contramarca IS NOT NULL then 1 else 0 end contramarca_tera
        ,case when conbq.ato_contramarca IS NOT NULL then 1 else 0 end contramarca_old -- Nueva
        ,case when prueba.ato_contramarca IS NOT NULL then 1 else 0 end contramarca -- Prueba
        ,case when com.ato_contramarca_prueba IS NOT NULL then 1 else 0 end prueba_contramarca-- Ato complaint
        ,ctx.config_id as config_id_ctx
        ,ctx.flow_type as flow_type_ctx
        ,dev.spmt_ref_id_nw as ref_id_nw_dev
        ,dev.device_id
        ,case when dev.browser_type IS NULL and dev.os IN ('android', 'iOS') then 'mobile nativo'
            when dev.browser_type = 'Desktop' and dev.os IN ('Windows', 'Linux', 'MacOS') then 'desktop'
            when dev.browser_type = 'Mobile' and dev.os IN ('Linux', 'iOS') then 'web mobile'
            else 'Sin Info / Desktop (cookies)'
        end as device_type
        ,dev.device_creation_date as device_creation_date_all_users
        ,dev.creation_date as device_creation_date
        ,dev.ip
        ,case 
            when re.result = 'APPROVED' and re.completed_challenges like '%FACE_VALIDATION%' and date_diff(cast(pyt.pyt_created_dt as datetime),cast(re.date_completed as datetime), minute) <= 30 
            then 1 else 0
        end as face_val_30m,
        lg.client_type as tipo_login
    FROM `meli-bi-data.WHOWNER.BT_MP_PAYOUTS` pyt
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp` tko 
            on pyt.payout_id = tko.action_value
            and tko.action_field_value IN ('tko_payouts','dt_payouts')
            and pyt.pyt_created_dt >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
         LEFT JOIN  --`meli-bi-data.WHOWNER.BT_MP_SCORING_TO_CUST` sco
        -- pre_scoring sco
         `warehouse-cross-pf.scoring.scoring_to_cust` sco
             on pyt.payout_id = sco.ref_id_nw -- sco.SCO_REF_ID_NW -- 
              and  sco.created_date >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
              -- and sco.flow_type = 'PO'
              LEFT JOIN `warehouse-cross-pf.scoring_po.reauth_status` re
            on re.spmt_ref_id_nw = pyt.payout_id 
            and re.spmt_created_date >= '2023-06-01' 
            and re.spmt_flow_type = sco.flow_type
        LEFT JOIN `meli-bi-data.WHOWNER.LK_CURRENCY_CONVERTION` cur
            on cur.tim_day = cast(pyt.pyt_created_dt  as date)
            and cur.sit_site_id = REGEXP_REPLACE(pyt.sit_site_id, ' ', '')
            and cur.tim_day >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN `warehouse-cross-pf.scoring_ext.fraude_po` mar
            -- on pyt.payout_id = mar.payout_id
               --  and PO_CREATED_DT >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca` con
            on pyt.payout_id = con.operation_id and con.tipo_op IN ('tko_payouts','dt_payouts')
        -- Contramarca Bq
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_bq` conbq
            on pyt.payout_id = conbq.operation_id and conbq.tipo_op IN ('tko_payouts','dt_payouts')
        -- Pruebita
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_prueba` prueba
            on pyt.payout_id = prueba.operation_id and prueba.tipo_op IN ('tko_payouts','dt_payouts')
        -- 23/02 ato complaint 
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_complaint` com
            on pyt.payout_id = com.operation_id and com.tipo_op IN ('tko_payouts','dt_payouts')
        LEFT JOIN `warehouse-cross-pf.scoring_po.context` ctx
            -- on stc.sc_cust_id = ctx.execution_id -- CAMBIO DE LA SCORING: ctx.scoring_id
            -- on stc.cust_id = ctx.execution_id -- Raro
            on pyt.payout_id = ctx.ref_id_nw
            and cast(ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `warehouse-cross-pf.scoring_po.device_ml` dev
            -- on stc.ref_id_nw = dev.spmt_ref_id_nw
            -- on stc.cust_id = dev.execution_id -- CAMBIO DE LA SCORING: dev.scoring_id
            on pyt.payout_id= dev.spmt_ref_id_nw
            and cast(dev.spmt_created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    ---- AGREGAR VARIABLE WEBVIEW -----
        LEFT JOIN `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` lg
            on pyt.payout_id = lg.operation_id 
            -- and lg.client_type = 'webview'
            and lg.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    WHERE EXTRACT(DATE FROM pyt.pyt_created_dt) >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    
)
;
