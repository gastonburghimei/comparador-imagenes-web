SELECT res.user_id,
       res.infraction_type,
       res.color_de_tarjeta,
       res.sentence_date,
       res.sit_site_id,
       res.detection_type,
       act.admin_id
FROM `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
INNER JOIN `WHOWNER.BT_ACTION_MR` act ON act.USER_ID = res.USER_ID
WHERE 1=1
  AND res.sentence_date >= '2025-01-01'
  AND res.sentence_date <= date_sub(current_date(), interval 30 DAY)
  AND res.infraction_type = 'ATO'
  AND res.color_de_tarjeta = '2'
  AND res.sentence_last_status = 'ACTIVE'
  AND act.ACTION_TYPE = 'close_case'
  QUALIFY 1=ROW_NUMBER() OVER (PARTITION BY RES.USER_ID ORDER BY ACT.ACTION_DATE DESC)

  --Agregamos lo de sacar cuentas canceladas

  SELECT res.user_id,
       res.infraction_type,
       res.color_de_tarjeta,
       res.sentence_date,
       res.sit_site_id,
       res.detection_type,
       act.admin_id
FROM `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` res
INNER JOIN `WHOWNER.BT_ACTION_MR` act ON act.USER_ID = res.USER_ID
LEFT JOIN `WHOWNER.LK_CUS_CUSTOMERS_DATA` cu on cu.cus_cust_id = act.USER_ID 
and cus_cust_status = 'deactive'
and REGEXP_CONTAINS(right(cu.CUS_EMAIL, 14), r'^[0-9]+$$')
and cast(substr(right(cu.CUS_EMAIL, 14),5,2) as string) between '01' and '12'
and cast(substr(right(cu.CUS_EMAIL, 14),7,2) as string) between '01' and '31'
WHERE 1=1
  AND res.sentence_date >= '2025-01-01'
  AND res.sentence_date <= date_sub(current_date(), interval 30 DAY)
  AND res.infraction_type = 'ATO'
  AND res.color_de_tarjeta = '2'
  AND res.sentence_last_status = 'ACTIVE'
  AND act.ACTION_TYPE = 'close_case' 
  and right(cu.CUS_EMAIL, 14) is null
  QUALIFY 1=ROW_NUMBER() OVER (PARTITION BY RES.USER_ID ORDER BY ACT.ACTION_DATE DESC)