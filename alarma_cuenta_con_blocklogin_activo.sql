WITH ultimo_aplicado AS
  (SELECT user_id,
          event_date,
          action
   FROM `atoratrack-7wgkhrgo7n7-furyid.tracks_public.backoffice_block_login`
   WHERE 1=1
     AND event_date >= '2025-01-01' QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY user_id
                                                                   ORDER BY event_date DESC) ),
     user_con_blocklogin AS
  (SELECT user_id,
          event_date,
          action
   FROM ultimo_aplicado
   WHERE 1=1
     AND action = 'block' )
SELECT bl.user_id,
       bl.event_date,
       bl.action,
       re.infraction_type,
       re.color_de_tarjeta,
       re.sentence_last_status
FROM user_con_blocklogin bl
LEFT JOIN `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_INFRACTIONS_NW` re ON bl.user_id=re.user_id
WHERE 1=1
  AND bl.event_date <= date_sub(current_date(), interval 5 DAY)
  AND re.sentence_date >= '2025-01-01'
  AND re.infraction_type = 'ATO'
  AND re.color_de_tarjeta = '2'
  AND re.sentence_last_status = ('DEACTIVE')