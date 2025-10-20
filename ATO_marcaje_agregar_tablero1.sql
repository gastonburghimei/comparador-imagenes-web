---------------- PASO 1
CREATE or replace TABLE `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp_GB` as
    (
    SELECT 
        action_type
        ,action_field_value
        ,user_id
        ,action_date
        ,action_id
        ,CAST(action_value as NUMERIC) as action_value
    FROM `meli-bi-data.WHOWNER.BT_ACTION_MR` 
    WHERE
        action_type IN ('hacker_fraud','device_theft')
        and action_field_value IN ('tko_payouts', 'tko_withdraw', 'tko_payments','dt_payments','dt_payouts','dt_withdraw')
        and ACTION_DATE >=  DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
      QUALIFY 1 = row_number () OVER (PARTITION BY CAST(action_value as NUMERIC) ORDER BY action_date DESC) 
    )
    ;