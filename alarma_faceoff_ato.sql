SELECT *
FROM `meli-bi-data.SBOX_PFFINTECHATO.faceoff_ato_ant_post`
WHERE 1=1
  AND cast((op_datetime + interval 1 HOUR) AS date) > date_sub(current_date(), interval 2 DAY)
  AND cast((op_datetime + interval 1 HOUR) AS date) < current_date() 
;

--Hay un job corriendo en data suite con el nombre
--clasif_ato_faceoff
--que alimenta a esta alarma