select a.user_id,
a.action_date,
c.gca_cust_id,
c.fecha_cierre_caso
from `meli-bi-data.WHOWNER.BT_ACTION_MR` a
  left join `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` c
where 1=1
and a.action_date > '2025-01-01'
and c.sit_site_id = 'MLA'
order by 2,4 desc

--Join con la ato_bq

select *bq,
      ac_action_date
from `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` bq
  left join `meli-bi-data.WHOWNER.BT_ACTION_MR` ac
    on bq.cust_id = ac.user_id
where 1=1
  and ac.action_date > '2025-01-01'
order by bq.op_datetime desc
limit 500