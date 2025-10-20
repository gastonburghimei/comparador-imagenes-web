select GCA_SUBTYPE, count(distinct GCA_ID) 
from `meli-bi-data.WHOWNER.BT_FRD_GENERAL_CASES_MANUALREW_EXP`
where GCA_TYPE_CASES = 'ato'
group by 1
order by 2 desc;

select GCA_TYPE_CASES, count(distinct GCA_ID)
from `meli-bi-data.WHOWNER.BT_FRD_GENERAL_CASES_MANUALREW_EXP`
group by 1
order by 2 desc
limit 50;

---------------------------------------------------------------------

---- Me traigo la tabla de los subtipos especificos con los que trabajamos y con nombre exacto del mes
select * from `meli-bi-data.WHOWNER.BT_FRD_GENERAL_CASES_MANUALREW_EXP`
where GCA_TYPE_CASES = 'ato'
and GCA_SUBTYPE in ('cuenta_de_hacker', 'ato_complaint', 'ato_chargebacks', 'efectos_colaterales',
'email_change', 'sf_ato', 'comerciales', 'cobertura_ato', 'recontactos_faq',
'ato_scoring_merchant_credit', 'cashout', 'credits', 'crypto_money_transfer', 'money_exchange',
'money_out', 'money_through', 'prepaid', 'qrc_payment', 'transfer', 'mediaciones',
'modelo_commerce_core', 'ato_commerce_bs', 'celula_commerce', 'ato_commerce_longtail',
'Modelo_ATO_FS', 'FCO_Seller', 'robo_device', 'recontactos_dto', 'robo_device_high_priority'
)
and cast(gca_date_created as date) > cast(current_date as date) - interval '3' day
-- and extract(year from cast(gca_date_created as date)) = 2024
-- and extract(month from cast(gca_date_created as date)) = 11
-------- Hasta ahi tengo 94649, no es la idea. Hay que achicar el universo
and gca_status = 'closed'
-------- Ahora tengo 92924, asi que voy a tomar los ultimos 3 dias y no el mes corriente
-------- Luego de filtrar por ultimos 3 dias tengo 5730 datos, lo que hace un universo considerable y
-------- a su vez no se carga de mas la query


---- Me traigo la tabla de los subtipos especificos con los que trabajamos y con nombre parecido
select * from `meli-bi-data.WHOWNER.BT_FRD_GENERAL_CASES_MANUALREW_EXP`
where GCA_TYPE_CASES = 'ato'
and (GCA_SUBTYPE like '%cuenta_de_hacker%' or '%ato_complaint%' or '%ato_chargebacks%' or 
'%efectos_colaterales%' or '%email_change%' or '%sf_ato%' or '%comerciales%' or '%cobertura_ato%'
or '%recontactos_faq%' or '%ato_scoring_merchant_credit%' or '%cashout%' or '%credits%' or
'%crypto_money_transfer%' or '%money_exchange%' or '%money_out%' or '%money_through%' or
'%prepaid%' or '%qrc_payment%' or '%transfer%' or '%mediaciones%' or '%modelo_commerce_core%' or
'%ato_commerce_bs%' or '%celula_commerce%' or '%ato_commerce_longtail%' or '%Modelo_ATO_FS%' or
'%FCO_Seller%' or '%robo_device%' or '%recontactos_dto%' or '%robo_device_high_priority%'
)
and cast(gca_date_created as date) > cast(current_date as date) - interval '3' day
-- and extract(year from cast(gca_date_created as date)) = 2024
-- and extract(month from cast(gca_date_created as date)) = 11
and gca_status = 'closed'
-- No matching signature for operator OR for argument types: BOOL, STRING, STRING, STRING, STRING,
-- STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
-- STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING.
-- Supported signature: BOOL OR ([BOOL, ...]) at [4:6]