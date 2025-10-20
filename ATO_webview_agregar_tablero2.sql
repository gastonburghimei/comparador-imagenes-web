-- PASO 3 DE LA ATO_BQ
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_temp_tmp_GB`  as
(
   SELECT DISTINCT
        wit.wit_withdrawal_id
        ,wit.cus_cust_id
        ,wit.wit_created_dt
        ,wit.sit_site_id
        ,wit.wit_created_datetime as wit_datetime
        ,wit.wit_withdrawal_amt as lc_op_amt -- 20/10
        ,wit.wit_withdrawal_amt /NULLIF(wit_withdrawal_dol_amt,0) as tipo_Cambio
        ,case 
            when wit.wit_status_id = 'approved' then 'A'
            when wit.wit_status_id = 'cancelled' then 'C'
            when wit.wit_status_id = 'in_process' then 'P'
            when wit.wit_status_id = 'rejected' and wit.wit_status_detail_id = 'by_high_risk' then 'I'
            when wit.wit_status_id = 'rejected' then 'R'
            else null
        end as wit_status_id
        ,wit.wit_withdrawal_dol_amt
        ,case when wit.wit_withdrawal_id is not null then 'RETIROS' else 'OTROS' end as producto -- Orne
        ,tko.action_field_value
        ,case when PRUEBA.ATO_CONTRAMARCA IS NOT NULL THEN -1 ELSE 1 END as marca_ato
        -- ,case when con.ato_contramarca IS NOT NULL then 1 else 0 end contramarca
        ,case when conbq.ato_contramarca IS NOT NULL then 1 else 0 end contramarca_old -- Nueva
        ,case when prueba.ato_contramarca IS NOT NULL then 1 else 0 end contramarca
        ,case when com.ato_contramarca_prueba IS NOT NULL then 1 else 0 end prueba_contramarca -- Ato complaint
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
            when re.result = 'APPROVED' and re.completed_challenges like '%FACE_VALIDATION%' and date_diff(cast(wit.wit_created_datetime as datetime),cast(re.date_completed as datetime), minute) <= 30
            then 1 else 0 end
            as face_val_30m,
            lg.client_type as tipo_login
        --- config_id_ctx,flow_type_ctx,ref_id_nw_dev,device_id,device_type,device_creation_date_all_users, device_creation_date,dev.ip
    FROM `meli-bi-data.WHOWNER.BT_MP_WITHDRAWALS` wit
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp` tko 
            on wit.wit_withdrawal_id = tko.action_value
            and tko.action_field_value IN ('tko_withdraw','dt_withdraw')
            and wit.wit_created_dt >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN `warehouse-cross-pf.scoring_ext.fraude_mo` mar
            -- on wit.wit_withdrawal_id = mar.wit_withdrawal_id
            -- and mar.wit_created_dt >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN -- `meli-bi-data.WHOWNER.BT_MP_SCORING_TO_CUST` sco
        -- pre_scoring sco
        -- `warehouse-cross-pf.scoring.scoring_to_cust` sco
        --     on wit.wit_withdrawal_id = sco.ref_id_nw -- sco.SCO_REF_ID_NW -- 
        --      and  sco.created_date >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca` con
            on wit.wit_withdrawal_id = con.operation_id and con.tipo_op IN ('tko_withdraw','dt_withdraw')
        -- Contramarca Bq
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_bq` conbq
            on wit.wit_withdrawal_id = conbq.operation_id and conbq.tipo_op IN ('tko_withdraw','dt_withdraw')
        -- Prueba
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_prueba` prueba
            on wit.wit_withdrawal_id = prueba.operation_id and prueba.tipo_op IN ('tko_withdraw','dt_withdraw')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_complaint` com
            on wit.wit_withdrawal_id = com.operation_id and com.tipo_op IN ('tko_withdraw','dt_withdraw')
        LEFT JOIN `warehouse-cross-pf.scoring_po.context` ctx
            -- on stc.sc_cust_id = ctx.execution_id -- CAMBIO DE LA SCORING: ctx.scoring_id
            -- on stc.cust_id = ctx.execution_id -- Raro
            on wit.wit_withdrawal_id  = ctx.ref_id_nw
            and cast(ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `warehouse-cross-pf.scoring_po.device_ml` dev
            -- on stc.ref_id_nw = dev.spmt_ref_id_nw
            -- on stc.cust_id = dev.execution_id -- CAMBIO DE LA SCORING: dev.scoring_id
            on wit.wit_withdrawal_id  = dev.spmt_ref_id_nw
            and cast(dev.spmt_created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `warehouse-cross-pf.scoring_po.reauth_status` re
            on re.spmt_ref_id_nw = wit.wit_withdrawal_id
            and re.spmt_created_date >= '2023-06-01' 
    ---- AGREGAR VARIABLE WEBVIEW -----
        LEFT JOIN `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` lg
            on wit.wit_withdrawal_id = lg.operation_id
            -- and lg.client_type = 'webview'
            and lg.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    WHERE wit.wit_created_dt >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)

)
;