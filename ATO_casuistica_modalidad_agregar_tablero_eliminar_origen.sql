---------------- PASO 2
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
        ,tko.action_date
        ,tko.action_id
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
        end as face_val_30m
        ,lg.client_type as tipo_login
        ,perf.casuistica_ato as casuistica
        ,perf.modalidad_ato as modalidad
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
        ---- AGREGAR VARIABLE TIPO_LOGIN (para identificar webviews) -----
        LEFT JOIN `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` lg
             on pyt.payout_id = lg.operation_id 
            -- and lg.client_type = 'webview'
             and lg.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        ---- AGREGAR CASUISTICA Y MODALIDAD -----
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.perfilado_usuarios_ATO` perf
             on pyt.payout_id = perf.operation_id
             and perf.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    WHERE EXTRACT(DATE FROM pyt.pyt_created_dt) >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    
)
;

---------------- PASO 3
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
        ,tko.action_date
        ,tko.action_id
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
            as face_val_30m
            ,lg.client_type as tipo_login
            ,perf.casuistica_ato as casuistica
            ,perf.modalidad_ato as modalidad
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
        ---- AGREGAR VARIABLE TIPO_LOGIN (para identificar webviews) -----
         LEFT JOIN `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` lg
             on wit.wit_withdrawal_id = lg.operation_id
             -- and lg.client_type = 'webview'
             and lg.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        ---- AGREGAR CASUISTICA Y MODALIDAD -----
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.perfilado_usuarios_ATO` perf
             on wit.wit_withdrawal_id = perf.operation_id
             and perf.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    WHERE wit.wit_created_dt >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)

)
;

---------------- PASO 4
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.pay_temp_tmp_GB`  as
(
   SELECT
        pay.pay_payment_id
        ,pay.sit_site_id
        ,pay.cus_cust_id_sel 
        ,pay.cus_cust_id_buy 
        ,pay.pay_created_dt
        ,pay.creation_date as pay_created_datetime 
        ,pay.pcc_status  as status_id
        ,pay.PAY_PM_TYPE_ID as pay_payment_type
        ,pay.PAY_TRANSACTION_AMT as lc_op_amt
        ,pay.PAY_TOTAL_PAID_DOL_AMT
		    ,case when pay.PAY_TOTAL_PAID_DOL_AMT = 0 then 0 else (pay.PAY_TRANSACTION_AMT)/(pay.PAY_TOTAL_PAID_DOL_AMT) end as tipo_cambio
        ,tko.action_field_value
        ,tko.action_date
        ,tko.action_id
        -- ,case when pay.PAY_PM_TYPE_ID IN ('bank_transfer') then 'PI' else pay.flow_type end as flow_type
        ,case when pay.PAY_OPERATION_TYPE_ID = 'account_fund' and pay.PAY_PM_TYPE_ID IN ('bank_transfer') then 'PI' 
              when pay.PAY_PM_TYPE_ID = 'account_money' then 'MT' when pay.PAY_PM_TYPE_ID in ('credit_card','debit_card') then 'MI' 
              else pay.flow_type 
        end as flow_type -- Orne
        ,pay.BUSINESS_UNIT as vertical
        -- ,pay.vertical
        ,case when pay.PRODUCT_TYPE is null then pay.BUSINESS_UNIT else pay.PRODUCT_TYPE end as producto
        -- ,case when pay.producto is null then pay.vertical else pay.producto end as producto -- Orne
        -- ,pay.producto
        ,pay.PAY_OPERATION_TYPE_ID
        ,pay.PAY_PM_DESC
        ,pay.config_id
        -- ,pay.pay_reason_id
        ,case when prueba.ato_contramarca IS NOT NULL then -1 else 1 end as marca_ato
        ,case when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '417400') = true then 'TC fisica'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '407843') = true then 'TC virtual'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '498581') = true then 'TC virtual'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '451996') = true then 'TC hibrida'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '477248') = true then 'TC fisica y virtual'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '47942800') = true then 'TC migracion'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '43125700') = true then 'TC migracion'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '554730') = true then 'TD virtual'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '510869') = true then 'TD virtual'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '407843') = true then 'TD hibrida'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '417401') = true then 'TD virtual'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '417402') = true then 'TD fisica'
        when CONTAINS_SUBSTR(PAY_CCD_FIRST_SIX_DIGITS, '542878') = true then 'TD fisica y virtual'
        else 'Otros' end as tarjeta_MP -- Agregada el 12/12/24 con Martu y Ale
        ,datetime_diff(pay_created_datetime,td.card_creation_dt,minute) as tarjeta_asociacion
        -- ,case when con.ato_contramarca IS NOT NULL then 1 else 0 end contramarca
        ,case when conbq.ato_contramarca IS NOT NULL then 1 else 0 end contramarca_old -- Nueva
        ,case when prueba.ato_contramarca IS NOT NULL then 1 else 0 end contramarca
        ,case when com.ato_contramarca_prueba IS NOT NULL then 1 else 0 end prueba_contramarca -- Ato complaint
        -- Verticales de Mercado Shops 6/06
        ,case when pay.MP_PROD_ID = 'C8B2P8D0B50KCIUOP1VG' then 'MSHOPS_GUEST'
              when pay.MP_PROD_ID = 'BTMA9QMHDNLM3TT37T60' then 'MSHOPS_MELI'
              else 'Others'
        end as vertical_mshops
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
            when re.result = 'APPROVED' and re.completed_challenges like '%FACE_VALIDATION%' and date_diff(cast(pay.creation_date as datetime),cast(re.date_completed as datetime), minute) <= 30
            then 1 else 0 end 
            as face_val_30m
            ,lg.client_type as tipo_login
            ,perf.casuistica_ato as casuistica
            ,perf.modalidad_ato as modalidad
    FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` pay
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp` tko 
            on pay.pay_payment_id = tko.action_value
            and tko.action_field_value IN ('tko_payments','dt_payments')
            and pay.pay_created_dt >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        --LEFT JOIN `warehouse-cross-pf.scoring_ext.fraude_mt` mar_mt
            --on pay.pay_payment_id = mar_mt.pay_payment_id
            --and pay.PAY_PM_TYPE_ID = 'account_money'
            --and mar_mt.pay_created_dt  >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
       -- LEFT JOIN `warehouse-cross-pf.scoring_ext.fraude_mc` mar_mc
            -- on pay.pay_payment_id = mar_mc.pay_payment_id
            -- and pay.PAY_PM_TYPE_ID = 'digital_currency'
            -- and mar_mc.pay_created_dt  >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN -- `meli-bi-data.WHOWNER.BT_MP_SCORING_TO_CUST` sco
        -- pre_scoring sco
        -- `warehouse-cross-pf.scoring.scoring_to_cust` sco
        --     on pay.pay_payment_id = sco.ref_id_nw -- sco.SCO_REF_ID_NW -- 
        --     and  sco.created_date >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca` con
            on pay.pay_payment_id = con.operation_id and con.tipo_op IN ('tko_payments','dt_payments')
        -- Contramarca Bq
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_bq` conbq
            on pay.pay_payment_id = conbq.operation_id and conbq.tipo_op IN ('tko_payments','dt_payments')
        -- Prueba
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_prueba` prueba
            on pay.pay_payment_id = prueba.operation_id and prueba.tipo_op IN ('tko_payments','dt_payments')
        LEFT JOIN `meli-sbox.PFCARDS.cards_td_pcc_status` td
            on pay.pay_payment_id = td.pay_payment_id
            and td.pay_created_dt  >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_complaint` com
            on pay.pay_payment_id = com.operation_id and com.tipo_op IN ('tko_payments','dt_payments')
        -- Agregar status 24/01
        LEFT JOIN (SELECT pay_payment_id, b.card_creation_dt
                    FROM `meli-sbox.PFCARDS.cards_td_pcc_status` a
                    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_CARD` b
                      on a.ppd_card_id = b.card_id
                      and a.pay_created_dt  >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
                    WHERE card_type = 'fisica' ) crd
        on pay.pay_payment_id = crd.pay_payment_id 
        LEFT JOIN `warehouse-cross-pf.scoring_pay.context` ctx
            -- on stc.sc_cust_id = ctx.execution_id -- CAMBIO DE LA SCORING: ctx.scoring_id
            -- on stc.cust_id = ctx.execution_id -- Raro
            on pay.pay_payment_id = ctx.ref_id_nw
            and cast(ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `warehouse-cross-pf.scoring_pay.device_ml` dev
            -- on stc.ref_id_nw = dev.spmt_ref_id_nw
            -- on stc.cust_id = dev.execution_id -- CAMBIO DE LA SCORING: dev.scoring_id
            on pay.pay_payment_id = dev.spmt_ref_id_nw
            and cast(dev.spmt_created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `warehouse-cross-pf.scoring_pay.reauth_status` re
            on re.spmt_ref_id_nw = pay.pay_payment_id
            and re.spmt_created_date >= '2023-06-01' 
        ---- AGREGAR VARIABLE TIPO_LOGIN (para identificar webviews) -----
         LEFT JOIN `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` lg
             on pay.pay_payment_id = lg.operation_id
            -- and lg.client_type = 'webview'
             and lg.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        ---- AGREGAR CASUISTICA Y MODALIDAD -----
         INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.perfilado_usuarios_ATO` perf
             on pay.pay_payment_id = perf.operation_id
             and perf.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
     WHERE pay.pay_created_dt >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    QUALIFY 1 = row_number () OVER (PARTITION BY pay_payment_id ORDER BY pay_total_paid_dol_amt DESC)
    );

---------------- PASO 5
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.tcmp_temp_tmp_GB`  as
(
   SELECT
        tcmp.CCARD_PURCH_ID
        ,tcmp.cus_cust_id
        ,tcmp.sit_site_id
        ,tcmp.CCARD_PURCH_OP_DT as ccard_purch_op_datetime
        ,tcmp.AUD_INS_DT
        ,tcmp.CCARD_PURCH_AUTH_STATUS as status_id
        ,tcmp.CCARD_PURCH_OP_ORIG_AMT_USD
        ,CCARD_PURCH_OP_ORIG_AMT_LC as lc_op_amt
	    ,case when tcmp.CCARD_PURCH_OP_ORIG_AMT_USD = 0 then 0 else (tcmp.CCARD_PURCH_OP_ORIG_AMT_LC)/(tcmp.CCARD_PURCH_OP_ORIG_AMT_USD) end as tipo_cambio
        ,tko.action_field_value
        ,tko.action_date
        ,tko.action_id
        ,tcmp.CCARD_PURCH_AUTH_OP_ID
        ,datetime_diff(pay_created_datetime,td.card_creation_dt,minute) as tarjeta_asociacion
        ,case when prueba.ato_contramarca IS NOT NULL then -1 else 1 end as marca_ato
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
            when re.result = 'APPROVED' and re.completed_challenges like '%FACE_VALIDATION%' and date_diff(cast(tcmp.CCARD_PURCH_OP_DT as datetime),cast(re.date_completed as datetime), minute) <= 30
            then 1 else 0 end 
            as face_val_30m
            ,lg.client_type as tipo_login
            ,perf.casuistica_ato as casuistica
            ,perf.modalidad_ato as modalidad
    FROM `meli-bi-data.WHOWNER.BT_CCARD_PURCHASE` tcmp
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp` tko 
            on cast(tcmp.CCARD_PURCH_ID as STRING) = CAST(tko.action_value AS STRING)
            and tko.action_field_value IN ('tko_credit_card', 'dt_credit_cards')
            and tcmp.CCARD_PURCH_OP_DT >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca` con
            on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(con.operation_id AS STRING) and con.tipo_op IN ('tko_credit_card', 'dt_credit_cards')
        -- Contramarca Bq
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_bq` conbq
            on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(conbq.operation_id AS STRING) and conbq.tipo_op IN ('tko_credit_card', 'dt_credit_cards')
        -- Prueba
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_prueba` prueba
            on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(prueba.operation_id AS STRING) and prueba.tipo_op IN ('tko_credit_card', 'dt_credit_cards')
        LEFT JOIN `meli-sbox.PFCARDS.cards_td_pcc_status` td
            on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(td.pay_payment_id AS STRING)
            and td.pay_created_dt  >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_complaint` com
            on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(com.operation_id AS STRING) and com.tipo_op IN ('tko_credit_card', 'dt_credit_cards')
        -- Agregar status 24/01
        LEFT JOIN (SELECT pay_payment_id, b.card_creation_dt
                    FROM `meli-sbox.PFCARDS.cards_td_pcc_status` a
                    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_CARD` b
                      on a.ppd_card_id = b.card_id
                      and a.pay_created_dt  >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
                    WHERE card_type = 'fisica' ) crd
        on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(crd.pay_payment_id AS STRING)
        LEFT JOIN `warehouse-cross-pf.scoring_pay.context` ctx
            on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(ctx.ref_id_nw AS STRING)
            and cast(ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `warehouse-cross-pf.scoring_pay.device_ml` dev
            on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(dev.spmt_ref_id_nw AS STRING)
            and cast(dev.spmt_created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `warehouse-cross-pf.scoring_pay.reauth_status` re
            on CAST(re.spmt_ref_id_nw AS STRING) = CAST(tcmp.CCARD_PURCH_ID AS STRING)
            and re.spmt_created_date >= '2023-06-01' 
        ---- AGREGAR VARIABLE TIPO_LOGIN (para identificar webviews) -----
         LEFT JOIN `metrics-se-u50vqcmvvv5-furyid.atoFactoresDataSet.ATO_FACTORES_LOGIN` lg
             on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(lg.operation_id AS STRING)
            -- and lg.client_type = 'webview'
             and lg.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        ---- AGREGAR CASUISTICA Y MODALIDAD -----
         LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.perfilado_usuarios_ATO` perf
             on CAST(tcmp.CCARD_PURCH_ID AS STRING) = CAST(perf.operation_id AS STRING)
             and perf.op_datetime >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
     WHERE tcmp.CCARD_PURCH_OP_DT >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    QUALIFY 1 = row_number () OVER (PARTITION BY tcmp.CCARD_PURCH_ID ORDER BY tcmp.CCARD_PURCH_OP_ORIG_AMT_USD DESC)
    );