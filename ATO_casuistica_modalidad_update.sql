--VARIABLE MODALIDAD UPDATE
update `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
set bq.modalidad = p.modalidad 
from `meli-bi-data.SBOX_PFFINTECHATO.prueba_perfiles` p
where bq.operation_id = p.operation_id
--Eso lo agregamos al job en el step 8 y da ok, lo mismo hicimos con casuistica

SELECT distinct modalidad, 
count(*) 
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
where 1=1
and op_dt > '2023-12-31'
group by modalidad
;
--Esto es para chequear que tenemos ok las cantidades

