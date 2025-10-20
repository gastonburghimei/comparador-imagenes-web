select 
--extract(month from cast(gc.gca_date_created as date)) as mes,
gc.sit_site_id as site,
gc.gca_comment as comentario,
gc.gca_sla_slice as tiempo,
rest.admin_id as responsable,
rest.restriction,
--count(gc.gca_id) as casos
from `meli-bi-data.WHOWNER.BT_FRD_GENERAL_CASES_MANUALREW_EXP` gc left join 
`meli-bi-data.WHOWNER.BT_ACTION_MR` rest on rest.case_id = gc.gca_id and 
rest.action_type = 'close_case'
where 1=1
and cast(gc.gca_date_created as date) > cast(current_date as date) - interval '3' day
--and extract(year from cast(gc.gca_date_created as date)) = 2024
--and extract(month from cast(gc.gca_date_created as date)) in (11)
and gc.GCA_SUBTYPE in ('cuenta_de_hacker', 'ato_complaint', 'ato_chargebacks', 'efectos_colaterales',
'email_change', 'sf_ato', 'comerciales', 'cobertura_ato', 'recontactos_faq',
'ato_scoring_merchant_credit', 'cashout', 'credits', 'crypto_money_transfer', 'money_exchange',
'money_out', 'money_through', 'prepaid', 'qrc_payment', 'transfer', 'mediaciones',
'modelo_commerce_core', 'ato_commerce_bs', 'celula_commerce', 'ato_commerce_longtail',
'Modelo_ATO_FS', 'FCO_Seller', 'robo_device', 'recontactos_dto', 'robo_device_high_priority'
)
--group by 1,2,3
--order by 4 desc
---- Me quede con 6647 registros
;
-----------------------------------

with tabla as (
  select distinct(ACTION_TYPE), count(CASE_ID) as cantidad
from `meli-bi-data.WHOWNER.BT_ACTION_MR`
where 1=1
and CASE_TYPE = 'ato'
group by 1
order by 2 desc
)
select action_type, cantidad from tabla
where cantidad >= 20
;
-- Esto me trae los distintos action type que hay en la base, ordenados por cantidad de casos
-- pero filtrando para que me traiga los que al menos estan 20 veces.
