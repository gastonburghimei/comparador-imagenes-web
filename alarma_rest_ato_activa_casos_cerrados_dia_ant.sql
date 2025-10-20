--VERSION INICIAL

SELECT res.user_id,
       act.CASE_ID,
       act.ADMIN_ID,
       act.DATE_CREATED + interval 1 HOUR AS fecha_accion,
                                             res.infraction_type,
                                             res.color_de_tarjeta,
                                             res.SENTENCE_DATETIME,
                                             res.sit_site_id,
                                             res.detection_type,
                                             ac.DETAIL_COMMENT
FROM `WHOWNER.BT_ACTION_MR` act
INNER JOIN `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res ON act.USER_ID = res.USER_ID
LEFT JOIN `WHOWNER.BT_ACTION_MR` ac ON ac.CASE_ID = act.CASE_ID
AND ac.ACTION_TYPE = 'solution'
AND ac.DETAIL_COMMENT NOT IN ('NPS - Testeo ato - BR',
                              'NPS - Testeo ato - ES',
                              'ATO - PERFIL MALWARE - Cuenta intervenida - Pedido de datos - PT',
                              'ATO - Perfil Remoto - Pedido de datos - ES',
                              'ATO - Cuenta intervenida - Pedido de datos - PT',
                              'ATO - Cuenta intervenida - Pedido de datos - ES',
                              'ATO - Perfil Remoto - Pedido de datos - PT',
                              'ATO - PERFIL MALWARE - Cuenta intervenida - Pedido de datos - ES')
WHERE 1=1
  AND res.infraction_type = 'ATO'
  AND res.color_de_tarjeta IN ('2',
                               '3')
  AND res.sentence_last_status = 'ACTIVE'
  AND lower(act.CASE_TYPE) = 'ato'
  AND cast((act.DATE_CREATED + interval 1 HOUR) AS date) > date_sub(current_date(), interval 2 DAY)
  AND cast((act.DATE_CREATED + interval 1 HOUR) AS date) < current_date()
  AND act.ACTION_TYPE = 'close_case'
  AND res.SENTENCE_DATETIME <= act.DATE_CREATED

  --Evaluar lo que me paso Joaco, aca no esta actualizada

  --VERSION 2, LA QUE HIZO JOACO

SELECT res.user_id,
       act.CASE_ID,
       act.ADMIN_ID,
       act.DATE_CREATED + interval 1 HOUR AS fecha_accion,
       res.infraction_type,
       res.color_de_tarjeta,
       res.SENTENCE_DATETIME,
       res.sit_site_id,
       res.detection_type,
       ac.DETAIL_COMMENT,
       right(cu.CUS_EMAIL, 14) as fecha_baja_string
FROM `WHOWNER.BT_ACTION_MR` act
INNER JOIN `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res ON act.USER_ID = res.USER_ID
LEFT JOIN `WHOWNER.BT_ACTION_MR` ac ON ac.CASE_ID = act.CASE_ID
AND ac.ACTION_TYPE = 'solution'
AND ac.DETAIL_COMMENT NOT IN ('NPS - Testeo ato - BR',
                              'NPS - Testeo ato - ES')
left join `WHOWNER.LK_CUS_CUSTOMERS_DATA` cu on cu.cus_cust_id = act.USER_ID
and cus_cust_status = 'deactive'
and REGEXP_CONTAINS(right(cu.CUS_EMAIL, 14), r'^[0-9]+$$')
and cast(substr(right(cu.CUS_EMAIL, 14),5,2) as string) between '01' and '12'
and cast(substr(right(cu.CUS_EMAIL, 14),7,2) as string) between '01' and '31'
WHERE 1=1
  AND res.infraction_type = 'ATO'
  AND res.color_de_tarjeta IN ('2',
                               '3')
  AND res.sentence_last_status = 'ACTIVE'
  AND lower(act.CASE_TYPE) = 'ato'
  AND cast((act.DATE_CREATED + interval 1 HOUR) AS date) > date_sub(current_date(), interval 2 DAY)
  AND cast((act.DATE_CREATED + interval 1 HOUR) AS date) < current_date()
  AND act.ACTION_TYPE = 'close_case'
  AND res.SENTENCE_DATETIME <= act.DATE_CREATED
  AND (AC.DETAIL_COMMENT IS NULL
       OR AC.DETAIL_COMMENT NOT IN ('ATO - PERFIL MALWARE - Cuenta intervenida - Pedido de datos - PT',
                      'ATO - Perfil Remoto - Pedido de datos - ES',
                      'ATO - Cuenta intervenida - Pedido de datos - PT',
                      'ATO - Cuenta intervenida - Pedido de datos - ES',
                      'ATO - Perfil Remoto - Pedido de datos - PT',
                      'ATO - PERFIL MALWARE - Cuenta intervenida - Pedido de datos - ES',
                      'ATO - Pedido de datos: Manda mail en uso - PT',
                      'ATO - Pedido de datos: Manda mail en uso - ES',
                      'ATO - Cuenta intervenida - Pedido de datos: Envía datos incompletos - ES',
                      'ATO - Cuenta intervenida - Pedido de datos: Envía datos incompletos - PT'))
  and right(cu.CUS_EMAIL, 14) is null