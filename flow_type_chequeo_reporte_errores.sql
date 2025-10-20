--SCO 
select sco_flow_type,
sco_config_id,
case
  when sco_config_id = 'QRP' then 'PO-QRP'
  when sco_config_id = 'PIX' then 'PO-PIX'
  when sco_config_id = 'ATM' then 'PO-ATM'
  when sco_config_id = 'COT' then 'PO-COT'
  when sco_config_id = 'STD' then 'PO-STD'
  else sco_config_id
  end as vertical,
sco_points,
sco_profile_id,
sco_ref_id_nw,
sco_creation_date
from `meli-bi-data.WHOWNER.BT_MP_SCORING_TO_CUST` sco
where 1=1
and sco_ref_id_nw = 112287371584
--limit 1000
;

--CTX
select ctx.config_id,
ctx.flow_type,
ctx.ref_id_nw,
ctx.created_date
from `warehouse-cross-pf.scoring_po.context` ctx
where 1=1
and ctx.ref_id_nw = 112287371584
--limit 10
--NO ENCUENTRA
;

--RE
select spmt_ref_id_nw,
spmt_created_date,
spmt_flow_type
from `warehouse-cross-pf.scoring_po.reauth_status`
WHERE 1=1
AND spmt_ref_id_nw = 112287371584
--limit 10
;

--PAY
select *
from `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT`
where 1=1
and pay_payment_id = 112287371584
--limit 10
--NO ENCUENTRA
;

--STC
select ref_id_nw,
flow_type,
created_date
from `warehouse-cross-pf.scoring.scoring_to_cust`
WHERE 1=1
AND ref_id_nw = 112287371584
--limit 10
;

SELECT *
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
WHERE 1=1
AND OPERATION_ID = 112287371584
;

SELECT *
FROM `meli-bi-data.SBOX_PFFINTECHATO.consolidado_tmp`
WHERE 1=1
AND OPERATION_ID = 112287371584
;