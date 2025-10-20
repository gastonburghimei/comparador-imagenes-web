select * from `meli-bi-data.WHOWNER.BT_ACTION_MR` 
where 1=1
and case_type = 'restrictions_aml'
and extract(year from cast(last_updated as date)) = 2020
--and extract(month from cast(action_date as date)) = 11
and restriction <> 'null'
-- Sin acotar por case type tengo 32037 registros pero quiero achicar el universo
-- Ahora si, tengo 471 registros para estudiar
;