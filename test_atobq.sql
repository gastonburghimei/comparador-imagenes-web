---------------- PASO 1
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp` as
    (
    SELECT 
        action_type
        ,action_field_value
        ,user_id
        ,action_date
        ,CAST(action_value as NUMERIC) as action_value
    FROM `meli-bi-data.WHOWNER.BT_ACTION_MR` 
    WHERE
        action_type IN ('hacker_fraud','device_theft')
        and action_field_value IN ('tko_payouts', 'tko_withdraw', 'tko_payments','dt_payments','dt_payouts','dt_withdraw')
        and ACTION_DATE >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
      QUALIFY 1 = row_number () OVER (PARTITION BY CAST(action_value as NUMERIC) ORDER BY action_date DESC) 
    );

---------------- PASO 2
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.pyt_temp_tmp`  as
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
    WHERE EXTRACT(DATE FROM pyt.pyt_created_dt) >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    
)
;

---------------- PASO 3
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_temp_tmp`  as
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
    WHERE wit.wit_created_dt >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)

)
;

---------------- PASO 4
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.pay_temp_tmp`  as
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
     WHERE pay.pay_created_dt >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
    QUALIFY 1 = row_number () OVER (PARTITION BY pay_payment_id ORDER BY pay_total_paid_dol_amt DESC)
    );

--- Paso 5
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.pay_temp_tmp`
SET producto = 	'MONEY_TRANSFER_APP' 
where (vertical = 'OTHER' and producto is null and config_id = 'MTR' and PAY_OPERATION_TYPE_ID = 'money_transfer')
;

----- Paso 6
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.consolidado_tmp` as
    SELECT 
         operation_id
        ,cust_id
        ,status_id
        ,action_field_value
        ,action_date
        ,flow_type
        ,vertical
        ,vertical as producto -- son los mismos productos que lo que está como vertical
        -- ,null as producto
        ,null as payment_method
        ,null as pay_operation_type_id
        ,marca_ato
        -- ,contramarca
        ,contramarca
        ,contramarca_old
        ,prueba_contramarca -- Ato complaint
        ,site_id
        ,extract(date FROM op_dt) as op_dt
        ,op_dt as op_datetime
        ,op_amt
        ,lc_op_amt
        ,null as tarjeta_MP
        ,null as tarjeta_asociacion
        ,null as vertical_mshops
        ,config_id_ctx,flow_type_ctx,ref_id_nw_dev,device_id,device_type,device_creation_date_all_users, device_creation_date,ip
        ,face_val_30m
        ,tipo_login
    FROM `meli-bi-data.SBOX_PFFINTECHATO.pyt_temp_tmp`
    UNION ALL 
    SELECT 
         wit_withdrawal_id as operation_id
        ,cus_cust_id as cust_id
        ,wit_status_id as status_id
        ,action_field_value
        ,action_date
        ,'MO' as flow_type
        ,'RETIROS' as vertical
        ,producto -- ,null as producto
        ,null as payment_method
        ,null as pay_operation_type_id
        ,marca_ato
        -- ,contramarca
        ,contramarca
        ,contramarca_old
        ,prueba_contramarca -- Ato complaint
        ,sit_site_id as site_id
        ,wit_created_dt as op_dt
        ,wit_datetime as op_datetime
        ,wit_withdrawal_dol_amt as op_amt
        ,lc_op_amt
        ,null as tarjeta_MP
        ,null as tarjeta_asociacion
        ,null as vertical_mshops
        ,config_id_ctx,flow_type_ctx,ref_id_nw_dev,device_id,device_type,device_creation_date_all_users, device_creation_date,ip,
        face_val_30m, tipo_login
    FROM `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_temp_tmp`
    UNION ALL 
    SELECT  
         pay_payment_id as operation_id
        ,cus_cust_id_buy as cust_id
        ,status_id
        ,action_field_value
        ,action_date
        ,flow_type
        /*
        ,case
            when pay_operation_type_id != 'money_exchange' then pay_operation_type_id
            when pay_operation_type_id = 'money_exchange' then CONCAT(pay_operation_type_id, '-', pay_reason_id)
        end as vertical
        */
        ,vertical
        ,producto
        -- LA SOR NO TIENE PAY_PAYMENT_METHOD
        ,case when pay_payment_type = 'bank_transfer' and PAY_PM_DESC = 'DEBIN' then 'debin_transfer' else null end as payment_method
        ,pay_operation_type_id
        ,marca_ato
        -- ,contramarca
        ,contramarca
        ,contramarca_old
        ,prueba_contramarca -- Ato complaint
        ,sit_site_id as site_id
        ,pay_created_dt as op_dt
        ,pay_created_datetime as op_datetime
        ,pay_total_paid_dol_amt as op_amt
        ,lc_op_amt
        ,tarjeta_MP
        ,tarjeta_asociacion
        ,vertical_mshops
        ,config_id_ctx,flow_type_ctx,ref_id_nw_dev,device_id,device_type,device_creation_date_all_users, device_creation_date,ip,
        face_val_30m, tipo_login
    FROM `meli-bi-data.SBOX_PFFINTECHATO.pay_temp_tmp`
;

-- Paso 7
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.robos_min_max` as 
    SELECT 
         cust_id
        ,CAST(min(op_dt) as DATE) as min_op_dt
        ,CAST(max(op_dt) as DATE) as max_op_dt
        ,SUM(case when contramarca = 0 then op_amt else 0 end) as amt_robo_allstatus
    FROM `meli-bi-data.SBOX_PFFINTECHATO.consolidado_tmp`
    GROUP BY 1
;

---- Paso 8

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.prioridad_tmp` as 
    SELECT GCA_CUST_ID,
      MIN (pame.ORIGEN_ATO) as prioridad_min,
      MAX (pame.ORIGEN_ATO) as prioridad_max,
      MIN (fecha_apertura_caso) as fecha_minima, -- fecha minima para el conocimiento del evento
      MIN (red_social) as red_social_min,
      MAX (red_social) as red_social_max
    FROM `meli-bi-data.SBOX_PF_PAY_METRICS.CONSULTAS_ATO` pame
    GROUP BY 1
    ORDER BY 1
;

----- Paso 9

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.regla_prioridad_tmp` as 
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
    FROM `meli-bi-data.SBOX_PFFINTECHATO.prioridad_tmp` prioridad
;


-- DELETE ATO_BQ
DELETE FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
WHERE op_dt>= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
;
-- Paso 10 -> Creación ATO_BQ

INSERT INTO  `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
    SELECT
        con.* except(config_id_ctx,flow_type_ctx,ref_id_nw_dev,device_id,device_type,device_creation_date_all_users, device_creation_date,ip,face_val_30m, tipo_login)
        ,case 
            when mar.action_type = 'hacker_fraud' then 'ATO'
            when mar.action_type = 'device_theft' then 'DT'
            else null
        end as tipo_robo
        ,rob.min_op_dt
        ,rob.max_op_dt
        -- ,pat.saldo_max_previo
        -- ,pat.usuario_patot
        ,kyc.KYC_ENTITY_TYPE as kyc_entity_type
        ,kyc.KYC_KNOWLEDGE_LEVEL as kyc_knowledge_level
        ,seg.CUST_SEGMENT_CROSS as cust_segment_cross
        ,seg.CUST_SUB_SEGMENT_CROSS as cust_sub_segment_cross
        -- ,stc.sc_cust_id as scoring_id
        -- ,stc.cust_id as scoring_id
        ,stc.cust_id as scoring_id
        -- ,stc.user_id as scoring_id
        ,stc.profile_id as profile_id
        -- ,stc.exec_time
        -- ,stc.version
        ,stc.provider_id
        ,stc.industry
        ,stc.points
        ,stc.risk
        ,stc.created_date as stc_created_date
        ,stc.config_id as config_id_stc,
        con.config_id_ctx,
        con.flow_type_ctx,
        con.ref_id_nw_dev,
        case 
          when con.flow_type = 'MC' then devmc.device_id
          else con.device_id
        end as device_id,
        
        case 
            when con.flow_type = 'MC' and devmc.browser_type IS NULL and devmc.os IN ('android', 'iOS') then 'mobile nativo'
            when con.flow_type = 'MC' and devmc.browser_type = 'Desktop' and devmc.os IN ('Windows', 'Linux', 'MacOS') then 'desktop'
            when con.flow_type = 'MC' and devmc.browser_type = 'Mobile' and devmc.os IN ('Linux', 'iOS') then 'web mobile'
            else con.device_type
        end as device_type,
        
        case 
          when con.flow_type = 'MC' then devmc.device_creation_date
          else con.device_creation_date_all_users
        end as device_creation_date_all_users,
        
        case
          when con.flow_type = 'MC' then devmc.creation_date 
          else con.device_creation_date
          end as device_creation_date,
          
        case
          when con.flow_type = 'MC' then devmc.ip 
          else con.ip
          end as ip

        -- ,ctx.config_id as config_id_ctx
        -- ,ctx.flow_type as flow_type_ctx
        -- ,dev.spmt_ref_id_nw as ref_id_nw_dev
        -- ,dev.device_id
        -- ,case when dev.browser_type IS NULL and dev.os IN ('android', 'iOS') then 'mobile nativo'
        --     when dev.browser_type = 'Desktop' and dev.os IN ('Windows', 'Linux', 'MacOS') then 'desktop'
        --     when dev.browser_type = 'Mobile' and dev.os IN ('Linux', 'iOS') then 'web mobile'
        --     else 'unkown'
        -- end as device_type
        -- ,dev.device_creation_date as device_creation_date_all_users
        -- ,dev.creation_date as device_creation_date
        -- ,dev.ip
        --,ROUND(timestamp_diff(CAST(op_datetime AS TIMESTAMP), CAST(dev.creation_date AS TIMESTAMP), minute)/60,2) device_hour_age
        --,ROUND((timestamp_diff(CAST(op_datetime AS TIMESTAMP), CAST(dev.creation_date AS TIMESTAMP), minute)/60)/24,2) device_day_age
        --,ROUND((timestamp_diff(CAST(op_datetime AS TIMESTAMP), CAST(dev.creation_date AS TIMESTAMP), minute)/60)/24/30,2) device_month_age
        --,dev.popular_apps_count
        --,dev.popular_old_apps_count
        ,prio.prioridad_final
        ,prio.red_social_final as red_social
        ,case
            when prio.prioridad_final = 'MALWARE' then 'Troyano Bancario'
            when m.origen = 'MALWARE' then 'Troyano Bancario'
            when c.origen = 'Cookies' then 'Cookies'
            when prio.prioridad_final = 'ROBO_DV' then 'Robo Dispositivo'
            /*when h.origen = 'PHISHING' then 'Phishing'*/
            when prio.prioridad_final = 'PHISHING' then 'Phishing'
            when r.comment_remoto = 1 then 'Acceso Remoto'
            when f.origen = 'INFOSTEALERS CREDENCIALES' and s.Ato_posterior = 1 then 'Infostealers Credenciales'
            when prio.prioridad_final = 'ING_SOCIAL_llamado_Wapp' then 'Ingeniería Social'
            when prio.prioridad_final = 'ING_SOCIAL_redsocial' then 'Ingeniería Social'
            when prio.prioridad_final = 'ING_SOCIAL_sinespecificar' then 'Ingeniería Social'
            else prio.prioridad_final
        end as origen_final
        ,case
            when prio.prioridad_final = 'MALWARE' then 'Troyano Bancario faq'
            when m.origen = 'MALWARE' then 'Troyano Bancario Hades'
            when c.origen = 'Cookies' then 'Cookies'
            when prio.prioridad_final = 'ROBO_DV' then 'Robo Dispositivo'
            /*when h.origen = 'PHISHING' then 'Phishing Hellfish'
            when prio.prioridad_final = 'PHISHING' then 'Phishing faq'*/
            when r.comment_remoto = 1 then 'Acceso Remoto'
            when f.origen = 'INFOSTEALERS CREDENCIALES' and s.Ato_posterior = 1 then 'Infostealers Credenciales'
            when prio.prioridad_final = 'ING_SOCIAL_llamado_Wapp' then 'Ingeniería Social'
            when prio.prioridad_final = 'ING_SOCIAL_redsocial' then 'Ingeniería Social'
            when prio.prioridad_final = 'ING_SOCIAL_sinespecificar' then 'Ingeniería Social'
            else prio.prioridad_final
        end as origen_subfinal
        -- ,p.casuistica as casuisticas
        ,case 
            when (mar.action_type = 'device_theft' and con.flow_type IN ('MT','MO','PO','MI','MC')) then 1-- and (pat.usuario_patot = 0 or pat.usuario_patot = 1)) then 1
  	        when (mar.action_type = 'hacker_fraud' and con.flow_type IN ('MT','MO','PO')) then 1-- and pat.usuario_patot = 1) then 1
  	        when (mar.action_type = 'hacker_fraud' and con.flow_type IN ('MC', 'MI')) then 1-- and pat.usuario_patot = 0) then 1
	        else 0
	     end as reporte
        /*,tier1.since_Tier1
        ,tier2.since_Tier2
        ,case 
            when (op_dt <= since_Tier1 or (since_Tier1 is null and since_Tier2 is null)) then 'Tier 1'
            when (op_dt > since_Tier1 and (op_dt <= since_Tier2 or since_Tier2 is null)) then 'Tier 1'
            when op_dt < since_Tier2 and since_Tier1 is null then 'Tier 1'
            when op_dt > since_Tier2 then 'Tier 2'
          else null
        end as Tier_ATO*/
        /*,case 
            when ti.last_tier_status = 1 then tier
            else null
        end as Tier_ATO*/
        ,case
            when ti.tier = 1 then 'Tier 1'
            when ti.tier = 2 then 'Tier 2'
          else null
        end as Tier_ATO
        ,case
            when tier.policy_tier = 'Tier1' then 'Tier 1'
            when tier.policy_tier = 'Tier2' then 'Tier 2'
          else null
        end as Policy_Tier
        ,con.face_val_30m
        ,case
            when des.contramarca_final >= 1 then 1
            else 0
        end as marca_automatica
        ,con.tipo_login
    FROM `meli-bi-data.SBOX_PFFINTECHATO.consolidado_tmp` con
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robos_min_max` rob
            on con.cust_id = rob.cust_id
        -- LEFT JOIN robos_patot pat
            -- on con.cust_id = pat.cust_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp` mar
            on con.action_field_value = mar.action_field_value
            and con.operation_id = mar.action_value
        LEFT JOIN -- `meli-bi-data.WHOWNER.BT_MP_SCORING_TO_CUST` stc
        -- pre_scoring stc-- `warehouse-cross-pf.refined.models_risk_scoring`stc
        `warehouse-cross-pf.scoring.scoring_to_cust` stc
            on con.operation_id = stc.ref_id_nw
            and stc.flow_type in ('PO','MI','MT','MF','MC','MO')
            and con.flow_type = stc.flow_type
            and stc.created_date >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN `warehouse-cross-pf.scoring.context` ctx
            -- on stc.sc_cust_id = ctx.execution_id -- CAMBIO DE LA SCORING: ctx.scoring_id
            -- on stc.cust_id = ctx.execution_id -- Raro
           --  on stc.ref_id_nw = ctx.ref_id_nw
          --   and cast(ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN `warehouse-cross-pf.scoring.device_ml` dev
            -- on stc.ref_id_nw = dev.spmt_ref_id_nw
            -- on stc.cust_id = dev.execution_id -- CAMBIO DE LA SCORING: dev.scoring_id
           --  on stc.ref_id_nw = dev.spmt_ref_id_nw
            -- and cast(dev.spmt_created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `meli-bi-data.WHOWNER.LK_KYC_VAULT_USER` kyc
            on con.cust_id = kyc.cus_cust_id
        -- Cruzo con la prioridad
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.regla_prioridad_tmp` prio
            on con.cust_id = CAST (prio.gca_cust_id as numeric)
        -- Agrego lo de Tiers
        /*LEFT JOIN tier1
            on con.cust_id = tier1.user_id
        LEFT JOIN tier2
            on con.cust_id = tier2.user_id  */
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.tiers_ato` ti
            on con.cust_id = ti.user_id
        -- Lo nuevo de Tiers 10/4
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.policy_tier` tier
            on con.cust_id = tier.user_id
        -- Agrego si paso Face Valid 3/03
         LEFT JOIN `warehouse-cross-pf.scoring_mc.device_ml` devmc
            on con.operation_id = devmc.spmt_ref_id_nw
        LEFT JOIN `warehouse-cross-pf.scoring_po.reauth_status` re
            on re.spmt_ref_id_nw = con.operation_id 
            and re.spmt_created_date >= '2023-06-01' 
            and re.spmt_flow_type = stc.flow_type
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.desconocimientos_total` des
            on con.operation_id = des.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Foresee_Pablo` f
            on con.cust_id = f.user_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.saldos_foresee` s
            on con.cust_id = s.user_id
        /*LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Hellfish_Pablo` h
            on con.cust_id = h.user_id*/
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_remoto` r
            on con.cust_id = r.cust_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Malware_Pablo` m
            on con.cust_id = m.user_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_cookies` c
            on con.cust_id = c.cust_id
            and con.operation_id = c.ref_id_nw
        /*LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.perfil_users_tmp` p
            on con.cust_id = p.GCA_CUST_ID*/
        LEFT JOIN (
            SELECT
                CUS_CUST_ID,
                CUST_SEGMENT_CROSS,
                CUST_SUB_SEGMENT_CROSS
            FROM `meli-bi-data.WHOWNER.LK_MP_SEGMENTATION_SELLERS`
            QUALIFY 1 = row_number () OVER (PARTITION BY CUS_CUST_ID ORDER BY TIM_MONTH DESC)
        ) seg
            on con.cust_id = seg.cus_cust_id
    WHERE 1 = 1
    QUALIFY 1 = row_number () OVER (PARTITION BY operation_id ORDER BY op_amt DESC) 
;

-- 24/11
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET prioridad_final = 'indefinido' where prioridad_final is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET origen_final = 'indefinido' where origen_final is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET origen_subfinal = 'indefinido' where origen_subfinal is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET prioridad_final = 'ROBO_DV' where tipo_robo = 'DT'
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET origen_final = 'ROBO_DV' where tipo_robo = 'DT'
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET origen_subfinal = 'ROBO_DV' where tipo_robo = 'DT'
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_segment_cross = 'Sellers Mktpl' where cust_segment_cross = 'NA'
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_sub_segment_cross = 'Sellers Mktpl' where cust_sub_segment_cross = 'NA'
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_segment_cross = 'Payers' where cust_segment_cross is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_sub_segment_cross = 'Payers' where cust_sub_segment_cross is null
;

------------------------------

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET contramarca = 1 where operation_id IN (51805032164,50614433872,50614524278,
50618869432,52411303359,52330910293,52330715054,52329121714,52298847715,52183122188,52147577882,52147412268,52147009038,52146671882,52145909031,52145182084,52143770380,52132438583,52132205939,52124049802,52120254542,52107812928,51883674803,51886397258,52678428002,52677943210,52676832829,52667626419,52641719807,52625504108,52513144692,52503607649,52452271381,52451165388,52449206813,52437063925,52436541784,52547593772,52485145255,52485162738,52473065122,52473075145,52473103911,52473094611,52473081898,53072002420,53017913809,52986940254,52963321457,52861832432,52795731971,52674149231 -- Los payments de la manual que quedaron afuera
,51271836985,51271645072,51611403380,51611288391,51610204988,51611619172,51610791650,51610493775,51611249272,51610810570,51610523648,51094446088,51316818297,51316195427,51315728473,51316033308,51314847665,51521463203,51462089113,51426288965,51341683526,51277151324,51276361271,51150811429,51129621110 -- Los payouts de la manual que quedaron afuera
);

-- Casos face only Vale
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET contramarca = 1 where operation_id IN (60780297192,
60101316083,60199148976,59900382743,61191239274,61075714971,60608229529,60490643410,60232916422,60232693358,60134418119,61013125870,
60875185237,60840260849,60887432140,59923075132,59829031591,59829051265,59923146376,60204010222,60908924509,60908869025,60908864537,
61018898602,60908818169,60908733329,60908530027,61015819962,60253689974,60124864347,60223200964,60126152397,60882853679,60083361740,
60029994022,60026861586,59966649478,59789369341,59856376844,59762646391,59762458949,59758176097,59851191610,59711523201,59776539214,
59673578919,60146506065,60240416222,60141626261,60227393664,60227629198,60129520937,60129534803,60111034181,60191111362,60085613463,
60170870308,60062316687,60148468558,60124929118,60012937939,60003942623,60028983228,60128573787,60227572846,60561745892,60458546613,
60459218447,60720015816,60481526591,61479323906,61501334828,61039820659,60471459587,60471711593,60472071465,60472100563,60472910887,
60576880858,61089690783,61199835950,61046199167,61153715998,61018537919,61011704801,61089635582,60960296329,60912476667,61010072920,
60888950909,60995130684,60884332057,60994037078,61007049929,61143452864,61032491717,61143839404,61032549813,61144100622,61031855881,
61032445119,60114279467,61327522102,61195328039,61213173203,61283738882,61297547586,61326922524,61361020382,61184110145,61184235855,
61213217631,23086048266,61308776220,61183979223,61304240706,61297661010,61170347669,61384270360,61297689560,61170794175,61309018022,
61195324367,61229584979,61297646660,61304268240,61297850334,23100005681,23024904287,23124523711,23071231347,61014500926,60827285570,
61014360966,60847447481,60922677576,60630673569,60630727303,60535487049,60638543282,60870890922,60302521709,61343853032,60585549303,
60140892379,60140886381,60141247255,60141239255,60140889265,60139722567,60576025056,60526004072,60551382714,60525927462,60525987444,
60471373673,60567035660,60525987422,60544045863,61079802951,61201998980,60783000899,60935701064,60890641202,60937422330,60782586791,
60936740596,60828467593,60885952080,60891066236,60828636473,60936196894,60827220685,60936519998,60828362399,60364200649,61469814604,
61373436507,61351646617,60817430186,60828107082,60493090349,60479190057,60642316728,60638922153,60727360891,60980221009,61091828234,
60980595405,60735092835,61295678596,59450389998,60275175221,60258923657,60277493859,60254431561,59348553609,59362923217,60256685265,
60353279434,60274871303,60277231371,60375117264,60275010607,60274750315,59451869594,59440021631,59355284459,59444384244,60375072950,
59446515420,60349197834,59363568547,60323517834,59443208332,59449652288,59440211269,59448640100,59452124886,59453328118,59448208550,
58429832140,58550066916,59638160843,60220249900,60220390184,59105965845,59449460859,58475686681,61618214348,61563579952,61484722530,
61170817967,61382247450,61350647553,61172316033,61618400482,61598625430,61283992854,61431062328,61382310306,61350743519,61286220528,
61300947875,61500373411,61300475933,60490654660,61182216376,60926335646,60820176063,60927388236,60329489929,60403779271,61137148093,
60329360455,60431205782,60329339317,60431199982,60329297391,60766241090,61137137571,60431305582,60329501461,60659440767,61353555469,
60163100129,60544932414,61045895699,61045854459,60544313184,60460908028,60767253928,60136888807,60235390962,60235315764,60147208683,
60356971663,60388036523,60476466402,60441729135,60551016678,60475075182,60353661217,60490398512,60135691109,60267293050,60147252085,
60441969145,61046312003,60267024434,60234162012,60544887308,60476471170,60387999647,60448426927,60551221578,60642118965,60745313600,
60748715376,60638670943,60746217756,60639468047,60639000629,60638841361,60746631882,60745733302,60745195906,60745573674,60656472451,
60563890360,60466447173,60564300730,60571509154,60228574352,60208845624,59991607685,60086697288,59991748965,60129487059,60129395737,
60086797596,61276113314,61166363525,61274230382,61278026378,61164351001,60904076523,60880951643,60992852518,60883092203,61011784656,
60920437759,60992844402,60875721015,60995806926,60981978710,60961882319,60961770887,61072945700,60962410771,60441635541,62438352066,
62512575578,62372691498,62280584866,62106265801,62103634543,62055970215,62073587839,62069981581,62196827602,62074205691,62071583217,
62195079324,62194662770,62074333065,62012649745,62128582678,62002638391,61940872903,62074796294,61945109313,61911770751,61893463841,
62273739078,62076567828,62029808984,61808997739,61817959987,61780763811,61612069280,61613996188,60411470028,60614919816,60330808399,
60330902081,60410943506,61456483989,60412224680,60310376689,60309240665,60329875273
);

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET contramarca = 0 where operation_id IN (54572319827,54572151226,54571474174,54571004354,54570351654,
54570106765,54569821075,54569820321)
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET contramarca = 0 where operation_id = 59541189796
; -- Crypto

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET producto = config_id_stc where (site_id = 'MLA' and tipo_robo = 'DT' and flow_type = 'MI' and producto = 'OTROS')
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET producto = config_id_stc where (site_id = 'MLA' and tipo_robo = 'DT' and flow_type = 'MI' and producto = 'OTHER')
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET contramarca = 0 where pay_operation_type_id = 'money_exchange'
; -- No contramarcar estos casos

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET tipo_robo = 'DT' where operation_id IN (61176744672,61175466552,
61063450855,61063185913,61174998084,61060519559,61172218766,61171300050,61059677369,61168666664,60141247255,60141239255,60140889265,
60140892379,60140886381,61564347756,61447174033,61447216113,61447178933,61564196950,61564187328,61564160812,61447147401,61564097732,
61564105090,61564104300,61447033835,61447072429,61564029256,61563995036,61563998316,61446989465,61446974551,61563937712,61446948785,
61446926181,61563879134,61446893937,61446895199,61446866159,61563823368,61446845693,61446797335,61563736988,61563760216,61446740173,
61446672541,61563625610,61446559481,61563607108,61563591058,61446520001,61432978737,61545032744,61428087647,61428051189,61421219875,
61421132335,61420913209,61537493964,61537496212,61420811971,61317218895,61432099190,61317122161,61432030940,61432016172,61431839254,
61431791550,61316318689,61316263343,61316278305,61316239409,61431165988,61430938220,61429760646,61314652299,61314520599,61429587294,
63130389518,61551316790,63108962817,63247904468,63108879957)
;

