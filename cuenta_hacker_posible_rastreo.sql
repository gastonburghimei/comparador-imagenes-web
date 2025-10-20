select a.usuario as usuario_realiza_pago,
       id_contraparte as usuario_recibe_pago,
       a.tipo_operacion,
       a.id_operacion,
       a.estado_operacion,
       a.fecha_creada_operacion,
       round(sum(bq.op_amt),0) as monto_dolares
from `SBOX_PFFINTECHATO.resumen_operaciones` a
  LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
     on a.id_operacion = bq.operation_id
     and bq.contramarca = 0
where 1=1
  AND id_contraparte = '1917068106'
group by 1,2,3,4,5,6