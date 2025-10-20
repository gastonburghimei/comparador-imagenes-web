with
op_unif as (select user_id, operation_id,
            last_updated, device_id
            from `meli-bi-data.SBOX_PFFINTECHATO.Operaciones_Unificadas`
            ),
hifest as (select user_id, user_agent_raw, site_id,
           completed_elements, device_id, insertion_ts
           from `meli-bi-data.HIFEST.LOGIN`
           where 1=1
           and insertion_ts between '2024-07-01' and '2024-12-31'
           and lower(site_id) = 'mlb'
           --and lower(user_agent_raw) like '%mobile/15E148%' --> NO HAY DATOS PARA MOSTRAR
           and lower(user_agent_raw) like '%x11; intel mac os x 10_5_8; en-us%'
           and array_length (completed_elements) = 1 
           and 'totp' in UNNEST (completed_elements) --> Particularidades de la casuística
           ),
payments as (select CUS_CUST_ID_BUY, PAY_PAYMENT_ID, SIT_SITE_ID,
             PAY_TOTAL_PAID_DOL_AMT, PAY_STATUS_ID, PAY_APPROVED_DT
             from `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
             where 1=1
             and PAY_APPROVED_DT between '2024-11-01' and '2024-12-31'
             and SIT_SITE_ID = 'MLB'
             and PAY_PAYMENT_ID is not null
             )
Select
a.last_updated as fecha_creacion,
b.site_id as site,
b.user_agent_raw as user_agent,
c.CUS_CUST_ID_BUY,
c.PAY_PAYMENT_ID as operation_id,
--REPLACE(FORMAT('%t', c.PAY_TOTAL_PAID_DOL_AMT), '.', ',') AS formatted_dol_amount_PAY, 
c.PAY_STATUS_ID as status_PAY
from op_unif a --> contempla la totalidad de las operaciones en los diferentes flujos
  inner join hifest b
    on a.device_id = b.device_id
  left join
  payments c 
    on a.operation_id = c.PAY_PAYMENT_ID
group by 1,2,3,4,5,6 

-- Bajamos de 4 horas y 44 minutos a 7 minutos 33 segundos.
-- 5877 filas

-------------------
--ACTUALIZADO A LOS ERRORES QUE VIENE DANDO
with
op_unif as (select user_id, operation_id,
            last_updated, device_id
            from `meli-bi-data.SBOX_PFFINTECHATO.Operaciones_Unificadas`
            ),
hifest as (select user_id, user_agent_raw, site_id, 
           completed_elements, device_id, insertion_ts
           from `meli-bi-data.HIFEST.LOGIN`
           where 1=1
           and insertion_ts between '2024-07-01' and '2024-12-31'
           and lower(site_id) = 'mlb'
           --and lower(user_agent_raw) like '%mobile/15E148%' --> NO HAY DATOS PARA MOSTRAR
           and lower(user_agent_raw) like '%x11; intel mac os x 10_5_8; en-us%'
           and array_length (completed_elements) = 1 
           and 'totp' in UNNEST (completed_elements) --> Particularidades de la casuística
           ),
payments as (select CUS_CUST_ID_BUY, PAY_PAYMENT_ID, SIT_SITE_ID,
             PAY_TOTAL_PAID_DOL_AMT, PAY_STATUS_ID, PAY_APPROVED_DT
             from `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`
             where 1=1
             --and PAY_APPROVED_DT between '2024-11-01' and '2024-12-31'
             --Saco esta condicion para probar
             -- Todo indicaria que el campo fecha utilizado esta ok, luego de realizar las comparaciones con los otros campos fecha y consultando en Nutela la fecha de ingreso en el historial login.
             and SIT_SITE_ID = 'MLB'
             and PAY_PAYMENT_ID is not null
             )
Select
a.last_updated as fecha_creacion,
b.site_id as site,
b.user_agent_raw as user_agent,
c.CUS_CUST_ID_BUY,
c.PAY_PAYMENT_ID as operation_id,
--REPLACE(FORMAT('%t', c.PAY_TOTAL_PAID_DOL_AMT), '.', ',') AS formatted_dol_amount_PAY, 
c.PAY_STATUS_ID as status_PAY
from op_unif a --> contempla la totalidad de las operaciones en los diferentes flujos
  inner join hifest b
    on a.device_id = b.device_id
  left join
  payments c 
    on a.operation_id = c.PAY_PAYMENT_ID
group by 1,2,3,4,5,6