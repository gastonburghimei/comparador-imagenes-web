Select
c.CUS_CUST_ID_BUY,
a.last_updated as fecha_creacion,
c.PAY_PAYMENT_ID as operation_id,
b.site_id as site,
b.user_agent_raw as user_agent,
-- MANTENER DEPENDIENDO DE LA CONSULTA (POR PO O PAY) --
REPLACE(FORMAT('%t', c.PAY_TOTAL_PAID_DOL_AMT), '.', ',') AS formatted_dol_amount_PAY, c.PAY_STATUS_ID as status_PAY,
-- REPLACE(FORMAT('%t', d.PAYOUT_AMT), '.', ',') AS formatted_RE_amount_PO, d.PAYOUT_STATUS as status_PO
from
(
  SELECT
    spmt_ref_id_nw,
    last_updated,
    device_id
  FROM (
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_cc.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_ma.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_mc.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_mf.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_mo.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_pa.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_pay.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_pi.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_po.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_ra.device_ml` WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id FROM `warehouse-cross-pf.scoring_ss.device_ml` WHERE last_updated >= '2024-11-01'
  )
   group by 1,2,3) a --> contempla la totalidad de las operaciones en los diferentes flujos
  inner join `meli-bi-data.HIFEST.LOGIN` b
  on a.device_id = b.device_id 
  and b.user_agent_raw like '%Mobile/15E148%' 
  and b.user_agent_raw like '%X11; Intel Mac OS X 10_5_8; en-US%'
  and array_length (b.completed_elements) = 1  
  and 'totp' in UNNEST (b.completed_elements) --> Particularidades de la casuística
  left join
  -- MANTENER DEPENDIENDO DE LA CONSULTA (POR PO O PAY) --
  `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS` c on a.spmt_ref_id_nw = c.PAY_PAYMENT_ID
  -- `WHOWNER.BT_MP_PAYOUTS` d on a.spmt_ref_id_nw = d.PAYOUT_ID
  -- DEJAR SI CORRESPONDE CON CONTACTO POSTERIOR --
  -- inner join `meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO` e  on e.GCA_CUST_ID= cast (b.user_id as string) and cast (e.fecha_apertura_caso as timestamp) >= DATE_SUB(a.last_updated, INTERVAL 4 HOUR) --> diferencia horaria entre tablas
  where
  -- MANTENER DEPENDIENDO DE LA CONSULTA (POR PO O PAY) --
  c.PAY_PAYMENT_ID is not null
  -- d.PAYOUT_ID is not null
group by 1,2,3,4,5,6,7