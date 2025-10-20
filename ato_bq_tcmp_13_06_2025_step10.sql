-----------
----------- Tabla física para "contramarcar" ato mal marcados
-----------


CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.robo_00`  as (
    SELECT
        cust_id
        ,MIN(op_dt) as inicio_robo
        ,MAX(op_dt) as fin_robo
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
    GROUP BY 1
);


CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.robo_01`  as (
    SELECT
        operation_id
        ,cust_id
        ,flow_type
        ,vertical
        ,act.date_created
        ,act.action_field_value as tipo_op
        ,act.action_type
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
        LEFT JOIN `meli-bi-data.WHOWNER.BT_ACTION_MR`  act
            on CAST(ato.operation_id as STRING ) = CAST(REGEXP_SUBSTR(act.action_value, '[0-9]+') as STRING) -- CAMBIO DE NUMERIC A STRING POR LO DE TCMP
            and act.date_created >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
            --(add_months((current_date - extract(day FROM current_date)+1),-18))
            and act.action_type in ('hacker_fraud','device_theft', 'countermark')
            and act.action_field_value in ('tko_payouts', 'tko_withdraw', 'tko_payments','dt_payments','dt_payouts','dt_withdraw',
            'tko_credit_card', 'dt_credit_cards') -- AGREGAMOS TKO CREDIT CARD Y DT CREDIT CARDS POR LO DE TCMP
    ORDER BY act.date_created DESC
);

-- Retiros mal marcados
-- 3 tipos (manual, cb pre y cb post)
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_00`  as (
    SELECT 
       wit.wit_withdrawal_id
      ,wit.sit_site_id
      ,wit.wit_status_id
      ,wit.wit_status_detail_id
      ,wit.cus_cust_id
      ,wit.wit_created_dt
      ,wit.wit_withdrawal_dol_amt
      ,wit.wit_bank_acc_number
      ,wit.wit_bank_acc_bank_id
      ,wit.wit_bank_acc_id_number
      ,rob.inicio_robo
      ,rob.fin_robo
      ,ope.action_type
    FROM `meli-bi-data.WHOWNER.BT_MP_WITHDRAWALS` wit
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_01` ope
            on wit.wit_withdrawal_id = ope.operation_id
            and ope.tipo_op in ('tko_withdraw','dt_withdraw')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_00` rob
            on rob.cust_id = wit.cus_cust_id
    WHERE 1=1 
        and wit.wit_created_dt >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        --(add_months((current_date - extract(day FROM current_date)+1),-18))
)
;

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_01` as (
    SELECT 
        cero.cus_cust_id
        ,cero.wit_withdrawal_id
        ,count(
          case     
            when wit.cus_cust_id= cero.cus_cust_id and cero.inicio_robo >= wit.wit_created_dt +5 
              and cero.wit_bank_acc_number = wit.wit_bank_acc_number and cero.wit_bank_acc_bank_id = wit.wit_bank_acc_bank_id 
              and wit.wit_status_id='approved' and wit.wit_bank_acc_number is not null and wit.wit_bank_acc_number not in ('N/A') 
              and act.action_type is null then 1
            -- Nuevo 6/2 bancos digitales
            when wit.cus_cust_id= cero.cus_cust_id and cero.inicio_robo >= wit.wit_created_dt +5 
              and cero.wit_bank_acc_number = wit.wit_bank_acc_number and cero.wit_bank_acc_bank_id = wit.wit_bank_acc_bank_id 
              and wit.wit_status_id='approved' and wit.wit_bank_acc_number is not null and wit.wit_bank_acc_number not in ('N/A') 
              and act.action_type is null 
              and cero.wit_bank_acc_bank_id IN ('13935893','380','453') 
              and wit.WIT_CREATED_DT >= '2024-01-01' then 1 
          else null end) qty_cta_con_pre
        ,count(
          case     
            when kyc.cus_cust_id= cero.cus_cust_id and cero.wit_bank_acc_id_number = kyc.kyc_identification_number 
              and wit.wit_bank_acc_id_number is not null and wit.wit_bank_acc_id_number not in ('N/A') 
              AND CERO.WIT_BANK_ACC_BANK_ID NOT IN ('383','653','069','13935893','380','453') 
              -- (AND CERO.WIT_BANK_ACC_BANK_ID NOT IN ('13935893','380','453') and WIT_CREATED_DT >= '2024-01-01')-- agregue bancos digitales
              and cero.wit_withdrawal_id not in (17485369334, 17485268582) then 1
            when wit.cus_cust_id= cero.cus_cust_id and cero.inicio_robo >= wit.wit_created_dt +5 
              and cero.wit_bank_acc_id_number = wit.wit_bank_acc_id_number and wit.wit_bank_acc_id_number is not null 
              and wit.wit_bank_acc_id_number not in ('N/A') and act.action_type is null 
              AND CERO.WIT_BANK_ACC_BANK_ID NOT IN ('383','653','069','13935893','380','453')
              -- (AND CERO.WIT_BANK_ACC_BANK_ID NOT IN ('13935893','380','453') and WIT_CREATED_DT >= '2024-01-01') -- agregue bancos digitales
              and cero.wit_withdrawal_id not in (17485369334, 17485268582) then 1 
            else null end) qty_dni_titular
        ,count(case     
            when wit.cus_cust_id= cero.cus_cust_id and cero.fin_robo <= wit.wit_created_dt -5 
              and cero.wit_bank_acc_number = wit.wit_bank_acc_number and cero.wit_bank_acc_bank_id = wit.wit_bank_acc_bank_id 
              and wit.wit_status_id='approved' and wit.wit_bank_acc_number is not null and wit.wit_bank_acc_number not in ('N/A') 
              and act.action_type is null then 1     
            when wit.cus_cust_id= cero.cus_cust_id and cero.fin_robo <= wit.wit_created_dt -5 
              and cero.wit_bank_acc_id_number = wit.wit_bank_acc_id_number and wit.wit_bank_acc_id_number is not null 
              and wit.wit_bank_acc_id_number not in ('N/A') and act.action_type is null 
              AND CERO.WIT_BANK_ACC_BANK_ID NOT IN ('383','653','069') 
              and cero.wit_withdrawal_id not in (17485369334, 17485268582) then 1   
            -- Agrego 6/2 banco digitales
            when wit.cus_cust_id= cero.cus_cust_id and cero.fin_robo <= wit.wit_created_dt -5 
              and cero.wit_bank_acc_number = wit.wit_bank_acc_number and cero.wit_bank_acc_bank_id = wit.wit_bank_acc_bank_id 
              and wit.wit_status_id='approved' and wit.wit_bank_acc_number is not null and wit.wit_bank_acc_number not in ('N/A') 
              and act.action_type is null
              AND CERO.WIT_BANK_ACC_BANK_ID IN ('13935893','380','453') 
              and wit.WIT_CREATED_DT >= '2024-01-01' then 1 
            else null end) qty_cta_con_post
        -- Para cuenta conocida 21/3
        /*,count(
          case
            when mo.withdrawals_mo_iden_number_as_bank_account_iden_number_mo_user_id_as_user_id_to_doc_qty_5d_90d > 0 
            or mo.withdrawals_mo_bank_identifier_as_bank_account_identifier_mo_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_account_identifier_as_provider_destination_bank_account_identifier_po_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_iden_number_as_provider_destination_identification_number_po_user_id_as_user_id_qty_5d_90d > 0 
          then 1 else 0 end) cuenta_conocida
        ,count(
          case
            when ctx.bank_account_iden_number = ctx.src_id_number 
          then 1 else 0 end) doc_propio*/
    FROM `meli-bi-data.WHOWNER.BT_MP_WITHDRAWALS` wit
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_00` cero 
            on wit.cus_cust_id = cero.cus_cust_id
        LEFT JOIN `meli-bi-data.WHOWNER.LK_KYC_VAULT_USER` kyc
            on cero.cus_cust_id = kyc.cus_cust_id
        LEFT JOIN `meli-bi-data.WHOWNER.BT_ACTION_MR` act
            on wit.wit_withdrawal_id = CAST(act.action_value as STRING) -- CAMBIO DE NUMERIC A STRING POR LO DE TCMP
            and act.date_created >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
            and act.action_type in ('hacker_fraud','device_theft','countermark')
            and act.action_field_value in ('tko_withdraw','dt_withdraw')
        LEFT JOIN  `warehouse-cross-pf.scoring_po.vector_rules_mo` mo
            on mo.spmt_ref_id_nw = wit.wit_withdrawal_id
             and mo.spmt_flow_type = 'MO' 
        LEFT JOIN `warehouse-cross-pf.scoring_po.context` ctx
            on ctx.ref_id_nw = wit.wit_withdrawal_id
            and cast( ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
             and ctx.flow_type = 'MO'
    WHERE 1=1 
        and wit.wit_created_dt >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        --(add_months((current_date - extract(day FROM current_date)+1),-18))
    GROUP BY 1,2
)
;

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_02` as (
    SELECT 
        operation_id
        ,tipo_op
        ,case 
          when qty_cta_con_pre > 0 then 1 else 0 
        end as wit_cb_pre_contramarca
        ,case 
          when qty_cta_con_post > 0 then 1 else 0 
        end as wit_cb_post_contramarca
        ,case 
          when qty_dni_titular > 0 then 1 else 0 
        end as wit_dni_titular_contramarca
        ,case 
          when (contramarca_flow>0 or contramarca_dev>0) then 1 else 0 
        end as wit_device_contramarca
        ,case 
          when mal.wit_withdrawal_id is not null then 1 else 0 
        end as wit_manual_contramarca
        ,case 
          when ope.action_type = 'countermark' then 1 else 0
        end as wit_forense_contramarca
        -- Agrego cuenta conocida 21/3
        /*,case 
          when cuenta_conocida > 0 then 1 else 0 
        end as wit_cuenta_conocida
        ,case 
          when doc_propio > 0 then 1 else 0 
        end as wit_doc_propio*/
        -- ,case when (wit_cb_pre_contramarca > 0 or wit_cb_post_contramarca > 0 or wit_manual_contramarca >0 -- Hecho abajo
        -- or wit_dni_titular_contramarca >0 or wit_device_contramarca>0) then 1 else 0 end as wit_aggr_contramarca 
    FROM `meli-bi-data.SBOX_PFFINTECHATO.robo_01` rob
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_01`  wit
            on rob.operation_id = wit.wit_withdrawal_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_00`  ope
            on rob.operation_id = ope.wit_withdrawal_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Retiros_mal_marcados` mal 
            on rob.operation_id = mal.wit_withdrawal_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.contramarca_device` d
            on d.ref_id_nw = rob.operation_id and d.flow_type in ('MO')
    WHERE 1=1 
        and tipo_op in ('tko_withdraw','dt_withdraw')
)
;

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_02_bis`  as (
    SELECT 
      wi.*,
      case 
        when (wit_cb_pre_contramarca > 0 or wit_cb_post_contramarca > 0 or wit_manual_contramarca >0 
        or wit_dni_titular_contramarca >0 or wit_device_contramarca>0 or wit_forense_contramarca >0
        -- or wit_cuenta_conocida > 0 or wit_doc_propio > 0
        ) then 1 else 0 
      end as wit_aggr_contramarca
    FROM `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_02`  wi
);

-- Payouts mal marcados
-- 3 tipos (manual, cb pre y cb post)

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.payouts_00`  as (
    SELECT 
        pyt.payout_id
        ,pyt.sit_site_id
        ,pyt.payout_status
        ,pyt.pyt_status_detail
        ,pyt.cus_cust_id
        ,pyt.pyt_created_dt
        ,pyt.TOTAL_PAYOUT_AMT as total_apyout_amt_lc
        ,pyt.PYT_PRVDR_DATA_SRC_CPF_CNPJ
        ,pyt.PYT_PRVDR_DATA_DEST_IDENT_NUMBER
        ,pyt.PYT_PRVDR_DATA_DEST_BANK_ID
        ,pyt.PYT_PRVDR_DATA_DEST_NUMBER
        ,rob.inicio_robo
        ,rob.fin_robo
        ,ope.flow_type
        ,ope.vertical
        ,ope.action_type
    FROM `meli-bi-data.WHOWNER.BT_MP_PAYOUTS` pyt
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_01` ope
            on pyt.payout_id = ope.operation_id
            and ope.tipo_op  in ('tko_payouts','dt_payouts')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_00` rob
            on rob.cust_id = pyt.cus_cust_id
    WHERE 1=1 
        and CAST(pyt.pyt_created_dt as date) >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- (add_months((current_date - extract(day FROM current_date)+1),-13))
    ORDER BY ope.date_created DESC
)
;
-- 21/4
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.payouts_prueba`  as (
    SELECT base.*
        ,sum(
          case
          -- Cambio Joaco 7/02
            when pyt.PYT_PRVDR_DATA_DEST_BANK_ID in ('13935893','380','453') and pyt.pyt_created_dt >= '2024-02-01'
            and (mo.withdrawals_mo_iden_number_as_bank_account_iden_number_mo_user_id_as_user_id_to_doc_qty_5d_90d > 0 
            or mo.withdrawals_mo_bank_identifier_as_bank_account_identifier_mo_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_account_identifier_as_provider_destination_bank_account_identifier_po_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_iden_number_as_provider_destination_identification_number_po_user_id_as_user_id_qty_5d_90d > 0
            or ctx.bank_account_iden_number = ctx.src_id_number) then 0
          -- Lo que estaba
            when mo.withdrawals_mo_iden_number_as_bank_account_iden_number_mo_user_id_as_user_id_to_doc_qty_5d_90d > 0 
            or mo.withdrawals_mo_bank_identifier_as_bank_account_identifier_mo_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_account_identifier_as_provider_destination_bank_account_identifier_po_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_iden_number_as_provider_destination_identification_number_po_user_id_as_user_id_qty_5d_90d > 0
            or ctx.bank_account_iden_number = ctx.src_id_number
          then 1 
          else 0 end) cuenta_conocida
        ,sum(
          case
          -- Cambio Joaco 7/02
            when pyt.PYT_PRVDR_DATA_DEST_BANK_ID in ('13935893','380','453') and pyt.pyt_created_dt >= '2024-02-01' 
              and ctx.bank_account_iden_number = ctx.src_id_number then 0
            -- Anterior
            when ctx.bank_account_iden_number = ctx.src_id_number 
          then 1 else 0 end) doc_propio 
    FROM `meli-bi-data.SBOX_PFFINTECHATO.payouts_00`  base
        LEFT JOIN `warehouse-cross-pf.scoring_po.vector_rules_mo` mo
            on mo.spmt_ref_id_nw = base.payout_id
            and mo.spmt_flow_type = 'PO' and mo.spmt_config_id in ('TRF','PIX')
        LEFT JOIN `warehouse-cross-pf.scoring_po.context` ctx
            on ctx.ref_id_nw = base.payout_id
            and ctx.flow_type = 'PO' and ctx.config_id in ('TRF','PIX')
            and cast(ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- Agrego
        LEFT JOIN `meli-bi-data.WHOWNER.BT_MP_PAYOUTS` pyt
            on base.payout_id = pyt.payout_id
           and CAST(pyt.pyt_created_dt as date) >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- WHERE ope.tipo_op  in ('tko_payouts','dt_payouts')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    ORDER BY 1,5
);

-- 
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.payouts_01`  as (
    SELECT 
        base.cus_cust_id
        ,base.payout_id
        ,count(
          case     
            when base.vertical in ('PO-PIX') and pyt.cus_cust_id = base.cus_cust_id 
              and base.inicio_robo >= CAST(pyt.pyt_created_dt as date) +5 
              and base.PYT_PRVDR_DATA_DEST_NUMBER = pyt.PYT_PRVDR_DATA_DEST_NUMBER 
              and base.PYT_PRVDR_DATA_DEST_BANK_ID = pyt.PYT_PRVDR_DATA_DEST_BANK_ID 
              and pyt.PYT_PRVDR_DATA_DEST_NUMBER is not null 
              and pyt.PYT_PRVDR_DATA_DEST_BANK_ID not in ('N/A') and act.action_type is null then 1 
            -- Bancos digitales 6/2
            when base.vertical in ('PO-PIX') and pyt.cus_cust_id = base.cus_cust_id 
              and base.inicio_robo >= CAST(pyt.pyt_created_dt as date) +5 
              and base.PYT_PRVDR_DATA_DEST_NUMBER = pyt.PYT_PRVDR_DATA_DEST_NUMBER 
              and base.PYT_PRVDR_DATA_DEST_BANK_ID = pyt.PYT_PRVDR_DATA_DEST_BANK_ID 
              and pyt.PYT_PRVDR_DATA_DEST_NUMBER is not null 
              and pyt.PYT_PRVDR_DATA_DEST_BANK_ID in ('13935893','380','453') 
              and act.action_type is null 
              and pyt.pyt_created_dt >= '2024-02-01' then 1
          else null end) qty_cta_con_pre
        ,count(
          case     
            when base.vertical in ('PO-PIX','PO-QRP') 
              and base.PYT_PRVDR_DATA_SRC_CPF_CNPJ = base.PYT_PRVDR_DATA_DEST_IDENT_NUMBER 
              and base.PYT_PRVDR_DATA_SRC_CPF_CNPJ is not null 
              and base.PYT_PRVDR_DATA_DEST_IDENT_NUMBER not in ('N/A') 
              AND BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('60779196','32024691','61024352','34878543','21018182','32192325')
              and (BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('18236120','8561701') and pyt.pyt_created_dt >= '2023-10-01')
              -- Bancos digitales
              and (BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('13935893','380','453') and pyt.pyt_created_dt >= '2024-02-01')
              and base.cus_cust_id not in (674076502,113430507,205050623) 
              and (base.sit_site_id <> 'MLB' or base.PYT_PRVDR_DATA_DEST_BANK_ID in ('60701190','360305','60746948','0','90400888','416968','2038232','1181521','32402502','20855875','7237373','92702067','82639451','58160789','7679404','68900810','208','13935893','5463212','30306294')) then 1
            when base.vertical in ('PO-PIX') 
              and pyt.cus_cust_id= base.cus_cust_id 
              and base.inicio_robo >= CAST(pyt.pyt_created_dt as date) +5 
              and base.PYT_PRVDR_DATA_DEST_IDENT_NUMBER = pyt.PYT_PRVDR_DATA_DEST_IDENT_NUMBER 
              and pyt.PYT_PRVDR_DATA_DEST_IDENT_NUMBER is not null 
              and pyt.PYT_PRVDR_DATA_DEST_IDENT_NUMBER not in ('N/A') 
              and act.action_type is null 
              AND BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('60779196','32024691','61024352','34878543','21018182')
              and (BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('18236120','8561701') and pyt.pyt_created_dt >= '2023-10-01')
              -- Bancos digitales
              and (BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('13935893','380','453') and pyt.pyt_created_dt >= '2024-02-01')
              and base.cus_cust_id not in (674076502,113430507,205050623) 
              and (base.sit_site_id <> 'MLB' or base.PYT_PRVDR_DATA_DEST_BANK_ID in ('60701190','360305','60746948','0','90400888','416968','2038232','1181521','32402502','20855875','7237373','92702067','82639451','58160789','7679404','68900810','208','13935893','5463212','30306294')) then 1 
            else null end) qty_dni_titular 
        ,count(
          case     
            when base.vertical in ('PO-PIX') 
              and pyt.cus_cust_id= base.cus_cust_id 
              and base.fin_robo <= CAST(pyt.pyt_created_dt as date) -5 
              and base.PYT_PRVDR_DATA_DEST_NUMBER = pyt.PYT_PRVDR_DATA_DEST_NUMBER 
              and base.PYT_PRVDR_DATA_DEST_BANK_ID = pyt.PYT_PRVDR_DATA_DEST_BANK_ID 
              and pyt.PYT_PRVDR_DATA_DEST_NUMBER is not null 
              and pyt.PYT_PRVDR_DATA_DEST_BANK_ID not in ('N/A') 
              and act.action_type is null then 1     
            when base.vertical in ('PO-PIX') 
              and pyt.cus_cust_id= base.cus_cust_id 
              and base.fin_robo <= CAST(pyt.pyt_created_dt as date) -5 
              and base.PYT_PRVDR_DATA_DEST_IDENT_NUMBER = pyt.PYT_PRVDR_DATA_DEST_IDENT_NUMBER 
              and pyt.PYT_PRVDR_DATA_DEST_IDENT_NUMBER is not null 
              and pyt.PYT_PRVDR_DATA_DEST_IDENT_NUMBER not in ('N/A') 
              and act.action_type is null 
              AND BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('60779196','32024691','61024352','34878543','21018182')
              and (BASE.PYT_PRVDR_DATA_DEST_BANK_ID NOT IN ('18236120','8561701') and pyt.pyt_created_dt >= '2023-10-01')
              and base.cus_cust_id not in (674076502,113430507,205050623) 
              and (base.sit_site_id <> 'MLB' or base.PYT_PRVDR_DATA_DEST_BANK_ID in ('60701190','360305','60746948','0','90400888','416968','2038232','1181521','32402502','20855875','7237373','92702067','82639451','58160789','7679404','68900810','208','13935893','5463212','30306294')) then 1    
            -- Agrego bancos digitales 6/2
            when base.vertical in ('PO-PIX') 
              and pyt.cus_cust_id= base.cus_cust_id 
              and base.fin_robo <= CAST(pyt.pyt_created_dt as date) -5 
              and base.PYT_PRVDR_DATA_DEST_NUMBER = pyt.PYT_PRVDR_DATA_DEST_NUMBER 
              and base.PYT_PRVDR_DATA_DEST_BANK_ID = pyt.PYT_PRVDR_DATA_DEST_BANK_ID 
              and pyt.PYT_PRVDR_DATA_DEST_NUMBER is not null 
              and pyt.PYT_PRVDR_DATA_DEST_BANK_ID in ('13935893','380','453') 
              and pyt.pyt_created_dt >= '2024-02-01'
              and act.action_type is null then 1  
            else null end) qty_cta_con_post
        -- Agregar lo de cuenta conocida para pix 17/4
        /*,count(
          case
            when mo.withdrawals_mo_iden_number_as_bank_account_iden_number_mo_user_id_as_user_id_to_doc_qty_5d_90d > 0 
            or mo.withdrawals_mo_bank_identifier_as_bank_account_identifier_mo_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_account_identifier_as_provider_destination_bank_account_identifier_po_user_id_as_user_id_user_to_account_qty_5d_90d > 0 
            or mo.payouts_po_iden_number_as_provider_destination_identification_number_po_user_id_as_user_id_qty_5d_90d > 0
            or ctx.bank_account_iden_number = ctx.src_id_number
          then 1 else 0 end) cuenta_conocida
        ,count(
          case
            when ctx.bank_account_iden_number = ctx.src_id_number 
          then 1 else 0 end) doc_propio */
    FROM `meli-bi-data.WHOWNER.BT_MP_PAYOUTS` pyt
        INNER JOIN `meli-bi-data.SBOX_PFFINTECHATO.payouts_00` base 
            on pyt.cus_cust_id = base.cus_cust_id
        LEFT JOIN `meli-bi-data.WHOWNER.BT_ACTION_MR` act
            on pyt.payout_id = CAST(act.action_value as STRING) -- CAMBIO DE NUMERIC A STRING POR LO DE TCMP
            and act.action_type  in ('hacker_fraud','device_theft','countermark')
            and act.action_field_value  in ('tko_payouts','dt_payouts')
            and act.date_created >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- Para lo de cta conocida y doc
        /*LEFT JOIN  `warehouse-cross-pf.scoring.vector_rules_mo` mo
            on mo.ref_id_nw = pyt.payout_id
            and mo.flow_type = 'PO' and mo.config_id in ('TRF','PIX')
        LEFT JOIN `warehouse-cross-pf.scoring.context` ctx
            on ctx.ref_id_nw = pyt.payout_id
            and ctx.flow_type = 'PO' and ctx.config_id in ('TRF','PIX')*/
    WHERE 1=1 
        and CAST(pyt.pyt_created_dt as date) >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        --(add_months((current_date - extract(day FROM current_date)+1),-19))
    GROUP BY 1,2
)
;

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.payouts_02`  as (
    SELECT 
        pyt.operation_id
        ,tipo_op
        ,case 
          when qty_cta_con_pre > 0 then 1 else 0 
        end as pyt_cb_pre_contramarca
        ,case 
          when qty_cta_con_post > 0 then 1 else 0 
        end as pyt_cb_post_contramarca
        ,case 
          when qty_dni_titular > 0 then 1 else 0 
        end as pyt_dni_titular_contramarca
        ,case 
          when (contramarca_flow>0 or contramarca_dev>0) then 1 else 0 
        end as pyt_device_contramarca
        ,case 
          when mal.payout_id is not null then 1 else 0 
        end as pyt_manual_contramarca
        -- Lo de cta conocida 17/4
        ,case
          when cuenta_conocida > 0 then 1 else 0 
        end as pyt_cta_conocida
        ,case
          when doc_propio > 0 then 1 else 0 
        end as pyt_doc_propio
        ,case
          when cero.action_type = 'countermark' then 1 else 0
        end as pyt_forense_contramarca
        /*,case 
          when (pyt_manual_contramarca > 0 
          or pyt_cb_pre_contramarca > 0 
          or pyt_cb_post_contramarca > 0 
          or pyt_dni_titular_contramarca > 0 
          or pyt_device_contramarca>0) then 1 else 0 
        end as pyt_aggr_contramarca*/ -- Abajo
    FROM `meli-bi-data.SBOX_PFFINTECHATO.robo_01`  pyt
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payouts_01`  uno
            on uno.payout_id = pyt.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payouts_00`  cero
            on cero.payout_id = pyt.operation_id
        -- 19/4
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payouts_prueba`  prue
            on prue.payout_id = pyt.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Payouts_mal_marcados` mal 
            on mal.payout_id = pyt.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.contramarca_device` d
            on d.ref_id_nw=pyt.operation_id and d.flow_type in ('PO')
    WHERE 1=1 
        and tipo_op  in ('tko_payouts','dt_payouts')
)
;

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.payouts_02_bis`  as (
    SELECT 
      py.*,
      case 
        when (pyt_manual_contramarca > 0 
        or pyt_cb_pre_contramarca > 0 
        or pyt_cb_post_contramarca > 0 
        or pyt_dni_titular_contramarca > 0 
        or pyt_device_contramarca>0
        or pyt_cta_conocida > 0
        or pyt_doc_propio > 0
        or pyt_forense_contramarca >0
        ) then 1 else 0 
      end as pyt_aggr_contramarca
    FROM `meli-bi-data.SBOX_PFFINTECHATO.payouts_02`  py
);

-- Pagos mal marcados
-- 2 tipos: tarjeta fisica y manual
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.card_00`  as (
    SELECT
        fr.cus_cust_id_buy
        ,fr.cus_cust_id_sel -- 08/02
        ,fr.pay_payment_id
        /*,case when cast (PAY_CCD_FIRST_SIX_DIGITS as string) LIKE '417400%' then 'TC Fisica'
              when cast (PAY_CCD_FIRST_SIX_DIGITS as string) LIKE '407843%' then 'TC Virtual'
              when td.card_type = 'fisica' then 'TD Fisica'
              when td.card_type = 'virtual' then 'TD Virtual'
              when td.card_type = 'nfc' then 'TD NFC'
              else 'Otras'
        end as tarjeta_MP -- Agregado 08/02*/
        ,aut.SIT_SITE_ID as site_id -- 08/02
        ,aut.ppd_card_type
        ,aut.sit_site_id
    FROM `warehouse-cross-pf.scoring_ext.fraude_mt` fr
        INNER JOIN `meli-bi-data.WHOWNER.BT_PPD_AUTHORIZATION` aut 
            on fr.pay_payment_id = aut.pay_payment_id 
            and (fr.ato = 1 or fr.ato = -1)
    -- Cambio 08/02
    LEFT JOIN `meli-sbox.PFCARDS.cards_td_pcc_status` td
            on fr.pay_payment_id = td.pay_payment_id
    
    WHERE 1=1 
        and (fr.cus_cust_id_sel = 246176764 -- MLA
        or (fr.cus_cust_id_sel = 485058068 and (aut.ppd_card_type <> 'virtual' and aut.ppd_card_type <> 'wallet')) -- MLB
        or (fr.cus_cust_id_sel = 574451595 and (aut.ppd_card_type <> 'virtual' and aut.ppd_card_type <> 'wallet')) -- MLM
        )
)
;

CREATE or replace TABLE  `meli-bi-data.SBOX_PFFINTECHATO.card_01` as (
    SELECT
        fr.cus_cust_id_buy
        ,fr.pay_payment_id
        ,wit.sit_site_id
    FROM `warehouse-cross-pf.scoring_ext.fraude_mt` fr
        INNER JOIN `meli-bi-data.WHOWNER.BT_PPD_WITHDRAW` wit 
            on fr.pay_payment_id = wit.pay_payment_id 
            and (fr.ato = 1 or fr.ato = -1)
)
;

-- Cambie la payments por
CREATE or replace  TABLE `meli-bi-data.SBOX_PFFINTECHATO.pay_credits`  as (
    SELECT
        fr.pay_payment_id
        ,SUBSTR(pay.TPV_SEGMENT_ID, 1 , 50) as pay_reason_id_s
        --,SUBSTR(pay.pay_reason_id, 1 , 50) as pay_reason_id_s
        ,case
          when pay.TPV_SEGMENT_ID = 'Credits' then 'Credits'
          else null
        end as cumple_credits
        --,(REGEXP_SUBSTR(pay.TPV_SEGMENT_ID, 'Credits', 1, 1, 'i')) as cumple_credits
        --,(REGEXP_SUBSTR(pay_reason_id_s, 'Mercado Cr(é|e)dito', 1, 1, 'i')) as cumple_credits
    FROM `warehouse-cross-pf.scoring_ext.fraude_mt` fr
        INNER JOIN `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` pay
        --`meli-bi-data.WHOWNER.BT_PAY_PAYMENTS` pay Ya no existe la payments
            on fr.pay_payment_id = pay.pay_payment_id
            and (fr.ato = 1 or fr.ato = -1)
        --    and (REGEXP_SUBSTR(pay_reason_id_s, 'Mercado Cr(é|e)dito', 1, 1, 'i')) is not null
            and pay.cus_cust_id_sel in (1001231412,1001281207,1001278430,1001284244,1000581553,1000581591,200510359,244308899,315991617,338295247,364678562,377791844,387886269,397538085,398155093,398155102,398155127,398157372,531377907,531378299,531379258,531379409,531379445,531380394,531380571,531380809,531381759,531384021,681123963,742839220,772425983,772427432,772427504,772427524,772427619,772427686,772427720,772427740,772427754,772429031,788135824,788135881,232822596,239651129,239765986,281515213,286245549,300679707,377791222,399300627,399304074,399304599,507154075,507601938,571055537,571061848,571062227,571062534,648202421,689038411,689052444,689052520,689053273,695164247,743409419,772436956,772436986,772437032,772437101,772438664,772438717,756981467,757703911,757705368,254419645,377797332,467712962,468013784,468020948,468021276,468022718,742836878,768685452,772438101,772438137,772439531,788133771,788133780)
);

CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.payments_00`   as (
    SELECT 
        pay.operation_id
        ,tipo_op
        ,case 
          -- Agrego
            when (ato.tarjeta_MP IN ('TC Virtual', 'TD Virtual', 'TD NFC') and car.site_id IN ('MLA','MLB','MLM') and ato.min_op_dt >= '2024-01-01') then 0
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and car.site_id IN ('MLA','MLB','MLM') and ato.tarjeta_asociacion < 7*24*60 and min_op_dt >= '2024-01-01') then 0
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and car.site_id IN ('MLA','MLB','MLM') and ato.tarjeta_asociacion >= 7*24*60 and min_op_dt >= '2024-01-01') then 1
            -- Pedido Sampi
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion <= 20*24*60) then 0
          -- 
            when (car.pay_payment_id is not null or car_1.pay_payment_id is not null) then 1 
          else 0 
        end as pay_card_fisica_contramarca
        ,case 
          when cre.pay_payment_id is not null then 1 else 0 
        end as pay_pago_credits
        ,case 
          when mal.payment_id is not null then 1 else 0 
        end as pay_manual_contramarca
        ,case 
          when (contramarca_flow>0 or contramarca_dev>0) then 1 else 0 
        end as pay_device_contramarca
        /* Contramarca google play */
        /*,case
          when sor.CUS_CUST_ID_SEL = 280418453 then 1 else 0
        end as pay_gglpay*/
        /*,case 
          when (pay_card_fisica_contramarca > 0 or pay_manual_contramarca > 0 
          or pay_pago_credits > 0 or pay_device_contramarca>0) then 1 else 0 
        end as pay_aggr_contramarca*/ -- En otra tabla
        -- El free pass de MT 01/03
        /*,case 
          when (coalesce(payments_collector_id_as_collector_id_payer_id_as_payer_id_app_qty_not_ato_dt_180d,0)-coalesce(payments_collector_id_as_collector_id_payer_id_as_payer_id_app_qty_not_ato_dt_30d,0)>0 ) then 1 else 0-- cuenta conocida
        end as pay_cta_conocida*/
        ,case 
          when (coalesce(payments_collector_id_as_collector_id_payer_id_as_payer_id_app_qty_not_ato_dt_180d,0)-coalesce(payments_collector_id_as_collector_id_payer_id_as_payer_id_app_qty_not_ato_dt_30d,0)>0 ) and sor.producto IN ('MONEY_TRANSFER_APP','MONEY_TRANSFER_DESK') then 1 else 0-- cuenta conocida
        end as pay_cta_conocida
        ,case when pay.action_type = 'countermark' then 1 else 0
        end as pay_forense_contramarca
    FROM `meli-bi-data.SBOX_PFFINTECHATO.robo_01` pay
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.card_00` car
            on car.pay_payment_id = pay.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.card_01` car_1
            on car_1.pay_payment_id = pay.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.pay_credits` cre
            on cre.pay_payment_id = pay.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
            on pay.operation_id = ato.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Payments_mal_marcados` mal
        -- scoring.payments_mal_marcados mal
            on pay.operation_id = mal.payment_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.contramarca_device` d
            on d.ref_id_nw = pay.operation_id and d.flow_type in ('MI','MT','MC')
        -- 01/03 Agregar la free pass de MT
        LEFT JOIN `warehouse-cross-pf.scoring_pay.vector_terranova_wallet_cross` terra
            on terra.spmt_ref_id_nw = pay.operation_id and terra.spmt_flow_type = 'MT' -- and terra.created_date = current_date - 2
        LEFT JOIN `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` sor
            on sor.pay_payment_id = pay.operation_id -- 20/10
    WHERE 1=1 
        and tipo_op in ('tko_payments','dt_payments')
)
;

CREATE  or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.payments_00_bis`  as (
    SELECT pay.*,
      case 
        when (pay_card_fisica_contramarca > 0 or pay_manual_contramarca > 0 
          or pay_pago_credits > 0 or pay_device_contramarca>0 -- or pay_gglpay >0 
          or pay_cta_conocida > 0 or pay_forense_contramarca >0 ) then 1 else 0 
      end as pay_aggr_contramarca
    FROM `meli-bi-data.SBOX_PFFINTECHATO.payments_00` pay
);

-- 22/12 para traer lo viejo y anexarlo
CREATE or replace  TABLE `meli-bi-data.SBOX_PFFINTECHATO.contramarca_tera`   as(
    SELECT 
      operation_id,
      cust_id,
      ato_contramarca
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca`
);

-- 6/02 Desarrollo ato_complaints
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.consultas_ato`  as(
    SELECT 
      cast(gca_cust_id as STRING) as gca_cust_id, -- CAMBIO DE NUMERIC A STRING POR LO DE TCMP
      min (cast(fecha_apertura_caso as date)) as fecha_apertura_caso, -- fecha minima para el conocimiento del evento
      origen,
      case 
        when (origen = 'OTROS' or origen is null) then 'No' 
        else 'Yes'
      end as faq
    FROM `meli-bi-data.SBOX_PF_PAY_METRICS.CONSULTAS_ATO` 
    -- WHERE origen = 'OTROS' or origen is null -- No completan FAQ
    -- origen <> 'OTROS' and origen is not null -- Completan faq
    GROUP BY 1,3
    ORDER BY 1
);

-- 29/5 Agrego pagos guest
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.pagos_guest` as(
-- pagos_guest as (
    SELECT pay.MP_PROD_ID,
        pay.pay_payment_id,
        pay.sit_site_id,
        pay.cus_cust_id_sel, 
        pay.cus_cust_id_buy, 
        pro.MP_PROD_USER_TYPE
    FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` pay-- `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS` PAY
    LEFT JOIN `meli-bi-data.WHOWNER.BT_MP_PRODUCTS` pro
      on pay.mp_prod_id = pro.MP_PROD_ID and pay.pay_Created_dt >= DATE_SUB(DATE_SUB(current_date, INTERVAL 18 MONTH), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
);


CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_bq` as (
    SELECT 
        rob.operation_id
        ,rob.cust_id
        ,rob.tipo_op
        ,COALESCE(wit_cb_pre_contramarca,0) as wit_cb_pre_contramarca
        ,COALESCE(wit_cb_post_contramarca,0) as wit_cb_post_contramarca
        ,COALESCE(wit_dni_titular_contramarca,0) as wit_dni_titular_contramarca
        ,COALESCE(wit_device_contramarca,0) as wit_device_contramarca
        ,COALESCE(wit_manual_contramarca,0) as wit_manual_contramarca
        -- ,COALESCE(wit_cuenta_conocida,0) as wit_cuenta_conocida -- 21/3
        -- ,COALESCE(wit_doc_propio,0) as wit_doc_propio -- 21/3
        ,COALESCE(wit_aggr_contramarca,0) as wit_aggr_contramarca
        ,COALESCE(pyt_cb_pre_contramarca,0) as pyt_cb_pre_contramarca
        ,COALESCE(pyt_cb_post_contramarca,0) as pyt_cb_post_contramarca
        ,COALESCE(pyt_dni_titular_contramarca,0) as pyt_dni_titular_contramarca
        ,COALESCE(pyt_device_contramarca,0) as pyt_device_contramarca
        ,COALESCE(pyt_manual_contramarca,0) as pyt_manual_contramarca
        ,COALESCE(pyt_cta_conocida,0) as pyt_cta_conocida -- 21/3
        ,COALESCE(pyt_doc_propio,0) as pyt_doc_propio -- 21/3
        ,COALESCE(pyt_aggr_contramarca,0) as pyt_aggr_contramarca
        ,COALESCE(pay_card_fisica_contramarca,0) as pay_card_fisica_contramarca
        ,COALESCE(pay_pago_credits,0) as pay_pago_credits
        ,COALESCE(pay_device_contramarca,0) as pay_device_contramarca
        ,COALESCE(pay_manual_contramarca,0) as pay_manual_contramarca
        -- ,COALESCE(pay_gglpay,0) as pay_gglpay_contramarca
        ,COALESCE(pay_cta_conocida,0) as pay_cta_conocida -- 01/03
        ,COALESCE(pay_aggr_contramarca,0) as pay_aggr_contramarca
        ,case 
          when rob0.fin_robo <= '2022-12-29' then tera.ato_contramarca -- 22/12
          when rob0.fin_robo > '2022-12-29'and 
            (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 1 else 0 
        end as ato_contramarca
    FROM `meli-bi-data.SBOX_PFFINTECHATO.robo_01`  rob
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_00`  rob0 -- para agregarle la fecha y ponerlo como filtro 27/12
            on rob0.cust_id = rob.cust_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payments_00_bis`  pay
            on rob.operation_id = pay.operation_id
            and rob.tipo_op in ('tko_payments','dt_payments')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payouts_02_bis`  pyt
            on rob.operation_id = pyt.operation_id
            and rob.tipo_op  in ('tko_payouts','dt_payouts')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_02_bis`  wit
            on rob.operation_id = wit.operation_id
            and rob.tipo_op in ('tko_withdraw','dt_withdraw')
        -- Agrego tabla vieja 22/12
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.contramarca_tera`  tera
            on rob.operation_id = tera.operation_id
    WHERE 1 = 1
        and (case 
          when rob0.fin_robo <= '2022-12-29' then tera.ato_contramarca -- 22/12
          when rob0.fin_robo > '2022-12-29'and 
            (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 1 else 0 
        end ) > 0
        -- ato_contramarca > 0
    QUALIFY 1 = row_number() over (partition by rob.operation_id, rob.tipo_op order by rob.tipo_op)
    )
; 

-- Prueba 09/01
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_prueba` as (
    SELECT 
        rob.operation_id
        ,rob.cust_id
        ,rob.tipo_op
        ,COALESCE(wit_cb_pre_contramarca,0) as wit_cb_pre_contramarca
        ,COALESCE(wit_cb_post_contramarca,0) as wit_cb_post_contramarca
        ,COALESCE(wit_dni_titular_contramarca,0) as wit_dni_titular_contramarca
        ,COALESCE(wit_device_contramarca,0) as wit_device_contramarca
        ,COALESCE(wit_manual_contramarca,0) as wit_manual_contramarca
        ,COALESCE(wit_forense_contramarca,0) as wit_forense_contramarca
        -- ,COALESCE(wit_cuenta_conocida,0) as wit_cuenta_conocida -- 21/3
        -- ,COALESCE(wit_doc_propio,0) as wit_doc_propio -- 21/3
        ,COALESCE(wit_aggr_contramarca,0) as wit_aggr_contramarca
        ,COALESCE(pyt_cb_pre_contramarca,0) as pyt_cb_pre_contramarca
        ,COALESCE(pyt_cb_post_contramarca,0) as pyt_cb_post_contramarca
        ,COALESCE(pyt_dni_titular_contramarca,0) as pyt_dni_titular_contramarca
        ,COALESCE(pyt_device_contramarca,0) as pyt_device_contramarca
        ,COALESCE(pyt_manual_contramarca,0) as pyt_manual_contramarca
        ,COALESCE(pyt_forense_contramarca,0) as pyt_forense_contramarca
        ,COALESCE(pyt_aggr_contramarca,0) as pyt_aggr_contramarca
        ,COALESCE(pyt_cta_conocida,0) as pyt_cta_conocida -- 17/4
        ,COALESCE(pyt_doc_propio,0) as pyt_doc_propio -- 17/4
        ,COALESCE(pay_card_fisica_contramarca,0) as pay_card_fisica_contramarca
        ,COALESCE(pay_pago_credits,0) as pay_pago_credits
        ,COALESCE(pay_device_contramarca,0) as pay_device_contramarca
        ,COALESCE(pay_manual_contramarca,0) as pay_manual_contramarca
        -- ,COALESCE(pay_gglpay,0) as pay_gglpay_contramarca
        ,COALESCE(pay_cta_conocida,0) as pay_cta_conocida -- 01/03
        ,COALESCE(pay_forense_contramarca,0) as pay_forense_contramarca
        ,COALESCE(pay_aggr_contramarca,0) as pay_aggr_contramarca
        ,case when
            (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 1 
            when pag.MP_PROD_USER_TYPE = 'guest' then 1
            -- Pedidos Joaco 02/02
            when (ato.tarjeta_MP IN ('TC Virtual', 'TD Virtual', 'TD NFC') and site_id IN ('MLA','MLB','MLM') and min_op_dt >= '2024-01-01') then 0
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and site_id IN ('MLA','MLB','MLM') and tarjeta_asociacion < 7*24*60 and min_op_dt >= '2024-01-01') then 0
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and site_id IN ('MLA','MLB','MLM') and tarjeta_asociacion >= 7*24*60 and min_op_dt >= '2024-01-01') then 1
            -- Pedido Sampi
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion <= 20*24*60) then 0
            else 0 
        end as ato_contramarca
        ,ato.op_amt as ato_amt
        /*-- Desarrollo ato_complaints
        ,case when
            (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) 
            and (con.faq = 'No' and (con.fecha_apertura_caso <= (ato.op_dt -30) and con.fecha_apertura_caso >= (ato.op_dt +30) )) then 1
            -- and (faq = 'No' and ((ato.op_dt -30) <= con.fecha_apertura_caso <= (ato.op_dt +30)) ) then 1
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion >= 20*24*60) then 0
            else 0 
        end as ato_contramarca_prueba
        ,case 
          when (con.faq = 'No' and (con.fecha_apertura_caso <= ato.op_dt -30 and con.fecha_apertura_caso >= ato.op_dt +30 )) then 1
          /*when (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 2
          when (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) 
            and (con.faq = 'No' and (con.fecha_apertura_caso <= ato.op_dt -30 and con.fecha_apertura_caso >= ato.op_dt +30 )) then 3
            -- and (faq = 'No' and ((ato.op_dt -30) <= con.fecha_apertura_caso <= (ato.op_dt +30)) ) then 1*/
          /*when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion >= 20*24*60) then 0
          else 0 
        end as prueba_complaint */
    FROM `meli-bi-data.SBOX_PFFINTECHATO.robo_01`  rob
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_00`  rob0 -- para agregarle la fecha y ponerlo como filtro 27/12
            on rob0.cust_id = rob.cust_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payments_00_bis`  pay
            on rob.operation_id = pay.operation_id
            and rob.tipo_op in ('tko_payments','dt_payments')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payouts_02_bis`  pyt
            on rob.operation_id = pyt.operation_id
            and rob.tipo_op  in ('tko_payouts','dt_payouts')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_02_bis`  wit
            on rob.operation_id = wit.operation_id
            and rob.tipo_op in ('tko_withdraw','dt_withdraw')
        -- Agrego tabla vieja 22/12
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.contramarca_tera`  tera
            on rob.operation_id = tera.operation_id
        -- Para agregar lo de la tarjeta fisica
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
            on rob.operation_id = ato.operation_id
        -- Desarrollo ato_complaints
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.consultas_ato`  con
            on rob.cust_id = con.gca_cust_id
        -- Agrego pagos guest
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.pagos_guest` pag
        -- pagos_guest pag
            on rob.operation_id = pag.pay_payment_id
            
    WHERE 1 = 1
        and /*(case when
            (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 1 else 0 
        end ) > 0*/
        (case when (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 1 
              when pag.MP_PROD_USER_TYPE = 'guest' then 1
            -- Pedidos Joaco 02/02
              when (ato.tarjeta_MP IN ('TC Virtual', 'TD Virtual', 'TD NFC') and site_id IN ('MLA','MLB','MLM') and min_op_dt >= '2024-01-01') then 0
              when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and site_id IN ('MLA','MLB','MLM') and tarjeta_asociacion < 7*24*60 and min_op_dt >= '2024-01-01') then 0
              when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and site_id IN ('MLA','MLB','MLM') and tarjeta_asociacion >= 7*24*60 and min_op_dt >= '2024-01-01') then 1
            -- Pedido Sampi
              when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion <= 20*24*60) then 0
            else 0 
        end ) > 0 
        -- ato_contramarca > 0
    QUALIFY 1 = row_number() over (partition by rob.operation_id, rob.tipo_op order by rob.tipo_op)
    )
;

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_complaint` as (
    SELECT 
        rob.operation_id
        ,rob.cust_id
        ,rob.tipo_op
        ,COALESCE(wit_cb_pre_contramarca,0) as wit_cb_pre_contramarca
        ,COALESCE(wit_cb_post_contramarca,0) as wit_cb_post_contramarca
        ,COALESCE(wit_dni_titular_contramarca,0) as wit_dni_titular_contramarca
        ,COALESCE(wit_device_contramarca,0) as wit_device_contramarca
        ,COALESCE(wit_manual_contramarca,0) as wit_manual_contramarca
        -- ,COALESCE(wit_cuenta_conocida,0) as wit_cuenta_conocida -- 21/3
        -- ,COALESCE(wit_doc_propio,0) as wit_doc_propio -- 21/3
        ,COALESCE(wit_aggr_contramarca,0) as wit_aggr_contramarca
        ,COALESCE(pyt_cb_pre_contramarca,0) as pyt_cb_pre_contramarca
        ,COALESCE(pyt_cb_post_contramarca,0) as pyt_cb_post_contramarca
        ,COALESCE(pyt_dni_titular_contramarca,0) as pyt_dni_titular_contramarca
        ,COALESCE(pyt_device_contramarca,0) as pyt_device_contramarca
        ,COALESCE(pyt_manual_contramarca,0) as pyt_manual_contramarca
        ,COALESCE(pyt_cta_conocida,0) as pyt_cta_conocida -- 21/3
        ,COALESCE(pyt_doc_propio,0) as pyt_doc_propio -- 21/3
        ,COALESCE(pyt_aggr_contramarca,0) as pyt_aggr_contramarca
        ,COALESCE(pay_card_fisica_contramarca,0) as pay_card_fisica_contramarca
        ,COALESCE(pay_pago_credits,0) as pay_pago_credits
        ,COALESCE(pay_device_contramarca,0) as pay_device_contramarca
        ,COALESCE(pay_manual_contramarca,0) as pay_manual_contramarca
        ,COALESCE(pay_cta_conocida,0) as pay_cta_conocida -- 01/03
        ,COALESCE(pay_aggr_contramarca,0) as pay_aggr_contramarca
        ,case when
            (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 1 
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion <= 20*24*60) then 0
            else 0 
        end as ato_contramarca
        -- Desarrollo ato_complaints
        ,case when
            (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) 
            and (con.faq = 'No' and (con.fecha_apertura_caso >= ato.op_dt -30 or con.fecha_apertura_caso <= ato.op_dt +30 )) then 1
            -- and (faq = 'No' and ((ato.op_dt -30) <= con.fecha_apertura_caso <= (ato.op_dt +30)) ) then 1
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion <= 20*24*60) then 0
            else 0 
        end as ato_contramarca_prueba
        /*,case 
          when (con.faq = 'No' and (con.fecha_apertura_caso <= ato.op_dt -30 and con.fecha_apertura_caso >= ato.op_dt +30 )) then 1
          /*when (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) then 2
          when (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) 
            and (con.faq = 'No' and (con.fecha_apertura_caso <= ato.op_dt -30 and con.fecha_apertura_caso >= ato.op_dt +30 )) then 3
            -- and (faq = 'No' and ((ato.op_dt -30) <= con.fecha_apertura_caso <= (ato.op_dt +30)) ) then 1*/
          /*when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion >= 20*24*60) then 0
          else 0 
        end as prueba_complaint*/
    FROM `meli-bi-data.SBOX_PFFINTECHATO.robo_01`  rob
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_00`  rob0 -- para agregarle la fecha y ponerlo como filtro 27/12
            on rob0.cust_id = rob.cust_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payments_00_bis`  pay
            on rob.operation_id = pay.operation_id
            and rob.tipo_op in ('tko_payments','dt_payments')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.payouts_02_bis`  pyt
            on rob.operation_id = pyt.operation_id
            and rob.tipo_op  in ('tko_payouts','dt_payouts')
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_02_bis`  wit
            on rob.operation_id = wit.operation_id
            and rob.tipo_op in ('tko_withdraw','dt_withdraw')
        -- Agrego tabla vieja 22/12
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.contramarca_tera`  tera
            on rob.operation_id = tera.operation_id
        -- Para agregar lo de la tarjeta fisica
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
            on rob.operation_id = ato.operation_id
        -- Desarrollo ato_complaints
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.consultas_ato`  con
            on rob.cust_id = con.gca_cust_id
    WHERE 1 = 1
        and (case when
           (wit_aggr_contramarca > 0 or pyt_aggr_contramarca >0 or pay_aggr_contramarca >0) 
            and (con.faq = 'No' and (con.fecha_apertura_caso >= ato.op_dt -30 or con.fecha_apertura_caso <= ato.op_dt +30 )) then 1
            -- and (faq = 'No' and ((ato.op_dt -30) <= con.fecha_apertura_caso <= (ato.op_dt +30)) ) then 1
            when (ato.tarjeta_MP IN ('TD Fisica', 'TC Fisica') and tarjeta_asociacion <= 20*24*60) then 0
            else 0 
        end ) > 0
        -- ato_contramarca > 0
    QUALIFY 1 = row_number() over (partition by rob.operation_id, rob.tipo_op order by rob.tipo_op)
    )
;

