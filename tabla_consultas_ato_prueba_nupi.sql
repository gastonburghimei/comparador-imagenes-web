/* Entender que se queria obtener con esto para poder ir entendiendo la tabla cada vez mas 
y poder obtener consultas nosotros independientemente. */

select
count(distinct GCA_ID) AS cant_users,
extract(MONTH from fecha_apertura_caso) as mes,
subtype1
from `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO`
where true
and fecha_apertura_caso between '2024-09-01' and '2024-12-31'
and (
subtype1 in ('ato_complaint', 'robo_device', 'ato_complaint_high_priority')
or (subtype1 = 'sf_ato' and GCA_SECOND_SUBTYPE = 'derivaciones')
)
and SIT_SITE_ID = 'MLA'
group by 2, 3
order by mes asc;

-- Aca lo que obtenemos es la cantidad de usuarios por mes y subtype, en el ultimo cuatrimestre del 2024.
-- Buscaremos hacer una query que obtenga los usuarios segun condiciones importantes de la tabla.

/* Aca nos traemos todos los casos que se marcaron como ATO en lo que va del 2025 */

select
GCA_CUST_ID AS usuario,
fecha_apertura_caso as fecha,
vertical,
subtype1,
tipo_cierre,
ORIGEN_ATO,
gca_comment as comentario
from `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO`
where 1=1
and fecha_apertura_caso > '2025-01-01'
and marcaje_ato = 1
--and reincide = 0 para evaluar primeros casos o reincidentes
--and SIT_SITE_ID = 'MLA'
--group by 1
order by 2
;