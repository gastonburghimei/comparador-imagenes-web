-- PASO 6 DE LA ATO_BQ
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.consolidado_tmp_GB` as
    SELECT 
         operation_id
        ,cust_id
        ,status_id
        ,action_field_value
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
        ,null as vertical_mshops,
        config_id_ctx,flow_type_ctx,ref_id_nw_dev,device_id,device_type,device_creation_date_all_users, device_creation_date,ip,
        face_val_30m,
        tipo_login
    FROM `meli-bi-data.SBOX_PFFINTECHATO.pyt_temp_tmp`
    UNION ALL 
    SELECT 
         wit_withdrawal_id as operation_id
        ,cus_cust_id as cust_id
        ,wit_status_id as status_id
        ,action_field_value
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
        face_val_30m,
        tipo_login
    FROM `meli-bi-data.SBOX_PFFINTECHATO.withdrawals_temp_tmp`
    UNION ALL 
    SELECT  
         pay_payment_id as operation_id
        ,cus_cust_id_buy as cust_id
        ,status_id
        ,action_field_value
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
        face_val_30m,
        tipo_login
    FROM `meli-bi-data.SBOX_PFFINTECHATO.pay_temp_tmp`
;
