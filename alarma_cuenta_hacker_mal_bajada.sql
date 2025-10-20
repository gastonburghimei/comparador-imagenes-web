select a.casuistica_dc_desagrupado,
a.RULE_NAMES,
a.sentence_datetime,
a.site_id,
a.sentence_id,
a.user_id,
a.DETECTOR_NAMES,
a.DETECTOR_TYPES,
rnm.sentence_context_external_context_reason,
B.reason,
a.HAS_ROLLBACK_AND_REINSTATEMENT_30
from `meli-bi-data.SBOX_PF_MKT.trimestral_restricciones_consolidado_user` a
  left join `meli-bi-data.SBOX_PF_MKT.restricciones_nuevo_mundo` rnm
    on a.sentence_id = cast(rnm.id as int64)
    and rnm.sentence_type = 'INFRACTION'
  left join `meli-bi-data.WHOWNER.BT_ACTION_MR` B
    on a.sentence_id = b.sentence_id
where 1=1
  and a.casuistica = 'CUENTA_DE_HACKER'
  and Clasificacion_Restricciones = 'Permanent'
  and cast((a.sentence_date + interval 1 HOUR) AS date) > date_sub(current_date(), interval 3 DAY)
  AND cast((a.sentence_date + interval 1 HOUR) AS date) < current_date()
order by 3, 7
