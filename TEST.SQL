select a.*, b.id as id_login, b.insertion_ts as fecha_login, case when id is null then 0 else 1 end login_webview
from meli-bi-data.SBOX_PF_DA.ato_bq a
left join 
(SELECT id, user_id, device_id, insertion_ts 
    FROM `meli-bi-data.HIFEST.LOGIN` where user_agent_raw like '%wv%' 
        and code = 'grant_session' and time >='2023-03-01') b on a.cust_id=b.user_id and a.device_id=b.device_id
where contramarca=0
and op_dt>='2023-04-01'
and id is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.operation_id ORDER BY insertion_ts ASC) <= 1


SELEC ALL FROM from meli-bi-data.SBOX_PF_DA.ato_bq WHERE CUST ID 885175
-------

select * from `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` where op_datetime > '2024-10-01' and client_type = 'webview'