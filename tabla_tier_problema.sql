SELECT *
FROM `meli-bi-data.SBOX_PFFINTECHATO.tiers_ato`
where 1=1
--Where tier = 2
--and cust_id = 66364780 trae ato bq
--and cust_id = 1009892 NO trae ato bq
--and cust_id = 240182010 trae ato bq
--and cust_id = 501143726 trae ato bq
;

select operation_id,
cust_id,
op_dt,
tier_ato
from `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
where 1=1
and cust_id = 501143726
order by op_dt desc
;

select cust_id,
created_date,
Tier_ATO
from `meli-bi-data.SBOX_PFFINTECHATO.ato_credits_total`
where 1=1
and cust_id = 66364780
;

select *
from `audit-tier-i64eh1rtv1v-furyid.tierAuditDataset.tierAuditTableData`
limit 10