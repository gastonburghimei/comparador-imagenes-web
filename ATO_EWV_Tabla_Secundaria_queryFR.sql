CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.XXX_GB` as
(
  SELECT
    spmt_ref_id_nw,
    last_updated,
    device_id
  FROM (
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_cc.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_ma.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_mc.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_mf.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_mo.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_pa.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_pay.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_pi.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_po.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_ra.device_ml` 
        WHERE last_updated >= '2024-11-01'
    UNION ALL
    SELECT spmt_ref_id_nw, last_updated, device_id 
      FROM `warehouse-cross-pf.scoring_ss.device_ml` 
        WHERE last_updated >= '2024-11-01'
  )
   group by 1,2,3) 