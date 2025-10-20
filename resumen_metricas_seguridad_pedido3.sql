select user_id,
format_date('%Y-%m',sentence_date) as mes_restriccion,
infraction_type,
color_de_tarjeta,
sit_site_id,
from `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW`
where 1=1
and infraction_type = 'CUENTA_DE_HACKER'
and extract(year from cast(sentence_date as date)) = 2024
and sit_site_id = 'MLB'
group by 1,2,3,4,5
order by 2
;