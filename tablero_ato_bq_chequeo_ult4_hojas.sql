-- Chequear informacion actualizada de las ultimas 4 hojas del tablero que no dependen de nosotros

select *
from `meli-bi-data.SBOX_PFFINTECHATO.foresee_saldos`
order by processed_date desc
limit 5
;
-- Al 14 de febrero, habian corrido el job el 14/02 pero en el tablero la info trae hasta diciembre 2024

select *
from `meli-bi-data.SBOX_PFFINTECHATO.hellfish_saldos`
order by processed_date desc
limit 5
;
-- Al 14 de febrero, habian corrido el job el 05/02 pero en el tablero la info trae hasta enero 2025