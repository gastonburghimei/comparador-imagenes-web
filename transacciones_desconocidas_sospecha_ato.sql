begin
CREATE or replace TABLE `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ` 
PARTITION BY date(fecha_apertura_caso) as
(select
b.operation_id,
gca.fecha_apertura_caso,
gca.fecha_cierre_caso,
gca.gca_id,
gca.gca_cust_id,
gca.gca_subtype,
gca.gca_second_subtype,
gca.sit_site_id,
gca.eficiencia as eficiencia_ato,
gca.eficiencia_roboDv as eficiencia_dt,
bq.marca_ato as eficiencia1,
case when (gca.eficiencia=1 or gca.eficiencia_roboDv=1) then 1 else 0 end as eficiencia,
case when gca.gca_subtype like '%robo_device%' then 'DT'
when gca.gca_subtype like '%robo_device_high_priority%' then 'DT'
when gca.gca_subtype like '%robo_device,robo_device_high_priority%' then 'DT'
when gca.preg_robo_dispositivo='si' then 'DT'
when gca.gca_subtype like '%ato_complaint%' then 'ATO'
when gca.gca_subtype like '%ato_complaint_high_priority%' then 'ATO'
else 'ATO' end as tipo_FAQ,
case when gca.gca_owner_cases in ('internal') or gca.gca_comment like '%inconsistencias RM%' then "auto"
when gca.gca_owner_cases in ('second_score', 'second_score_tier2') then "auto"
when gca.gca_owner_cases not in ('taaccc', 'admin', 'manual_review', 'Exp_ATO_CX') then "manual" else 'auto' end as tipo_cierre
from `meli-bi-data.SBOX_PFFINTECHATO.desconocimientos_faq_atoConMonto` b
left join meli-bi-data.SBOX_PFFINTECHATO.CONSULTAS_ATO gca on b.id=gca.gca_id
left join meli-bi-data.SBOX_PFFINTECHATO.ato_bq bq on b.operation_id = bq.operation_id
where 1=1
and gca.gca_type_cases = 'ato'
and gca.gca_status='closed'
and cast(gca.fecha_apertura_caso as date) >= '2024-07-01'
and (gca.gca_subtype like '%ato_complaint%'
or gca.gca_subtype like '%ato_complaint_high_priority%'
or gca.gca_subtype like '%robo_device%'
or gca.gca_subtype like '%robo_device_high_priority%'
or gca.gca_subtype like '%robo_device,robo_device_high_priority%')
and b.operation_id is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY gca_id,operation_id ORDER BY fecha_apertura_caso DESC) <= 1
);

create or replace table `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ_payments`
PARTITION BY date(created_datetime) as
(
select
a.*,
c.pay_created_datetime as created_datetime,
cast (c.pay_status_id as string) as status_id,
cast (c.pay_status_detail_code as string) as status_detail_code,
c.pay_marketplace_id,
c.PAY_TOTAL_PAID_DOL_AMT as total_paid_dol_amt,
c.PRODUCT_TYPE,
c.FLOW_TYPE,
case when (c.BUSINESS_UNIT='MARKETPLACE' and c.PRODUCT_TYPE='CARRITO') then 'CARRITO'
when (c.BUSINESS_UNIT='MARKETPLACE' and c.PRODUCT_TYPE='MERCADO SHOPS CARRITO') then 'CARRITO'
when (c.BUSINESS_UNIT='MARKETPLACE' and c.PRODUCT_TYPE='MERCADO SHOPS DIRECT') then 'MARKETPLACE'
when (c.BUSINESS_UNIT='MARKETPLACE' and c.PRODUCT_TYPE='DIRECT') then 'MARKETPLACE'
when (c.BUSINESS_UNIT='ONLINE PAYMENTS' and c.SS_CUST_SEGMENT_CROSS='SMB' ) then 'OP - SMBs'
when (c.BUSINESS_UNIT='ONLINE PAYMENTS' and c.SS_CUST_SEGMENT_CROSS='BIG SELLERS' ) then 'OP - BIG SELLERS'
when (c.BUSINESS_UNIT='ONLINE PAYMENTS' and c.SS_CUST_SEGMENT_CROSS='LONGTAIL' ) then 'OP - LONG TAIL'
when (c.BUSINESS_UNIT='ONLINE PAYMENTS' and c.SS_CUST_SEGMENT_CROSS is null ) then 'OP - LONG TAIL'
when (c.BUSINESS_UNIT='ONLINE PAYMENTS' and c.SS_CUST_SEGMENT_CROSS='NA' ) then 'OP - NA' else 'No aplica' end as vertical_frenado,
case when flow_type='MI' then 1 else 0 end as aplica_CBK,
c.metodo_pago,
case when c.PAY_CCD_FIRST_SIX_DIGITS is not null then 1 else 0 end as tarjeta
from `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ` a
inner join (select cast(CREATION_DATE as datetime) as pay_created_datetime,pay_payment_id,pay_status_id, pay_status_detail_code, pay_marketplace_id, flow_type,BUSINESS_UNIT, SS_CUST_SEGMENT_CROSS,PRODUCT_TYPE, PAY_TOTAL_PAID_DOL_AMT, pay_payment_method_id as metodo_pago, PAY_CCD_FIRST_SIX_DIGITS
from `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` where cast(CREATION_DATE as date) >='2024-01-01') c on c.PAY_PAYMENT_ID = a.operation_id
where 1=1
and c.PAY_PAYMENT_ID is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY gca_id,operation_id ORDER BY fecha_apertura_caso DESC) <= 1
--order by gca_cust_id
);


create or replace table `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ_retiros`
PARTITION BY date(created_datetime) as
(
select
a.*,
PYT_CREATED_DT as created_datetime,
cast (PAYOUT_STATUS as string) as status_id,
CAST(PYT_STATUS_DETAIL AS STRING) as status_detail_code,
'Payout' as pay_marketplace_id,
PYT_AMT as total_paid_dol_amt,
'Payout' as PRODUCT_TYPE,
'PO' as FLOW_TYPE,
'No aplica' as vertical_frenado,
0 as aplica_CBK,
'No aplica' as metodo_pago,
0 as tarjeta,
from `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ` a
inner join (
  select pyt.PYT_CREATED_DT, pyt.PAYOUT_STATUS, pyt.PYT_STATUS_DETAIL, pyt.PAYOUT_ID, (pyt.total_payout_amt/cur.CCO_TC_VALUE) as PYT_AMT
  from `meli-bi-data.WHOWNER.BT_MP_PAYOUTS` pyt
  left join `meli-bi-data.WHOWNER.LK_CURRENCY_CONVERTION` cur on cur.tim_day = EXTRACT(DATE FROM pyt.pyt_created_dt) and cur.sit_site_id = REGEXP_REPLACE(pyt.sit_site_id, ' ', '')
  where cast(pyt.PYT_CREATED_DT as date) >= '2024-01-01'
) c on c.PAYOUT_ID = a.operation_id
where 1=1
and c.PAYOUT_ID is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY gca_id,operation_id ORDER BY fecha_apertura_caso DESC) <= 1
);

create or replace table `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ_withdrawals`
PARTITION BY date(created_datetime) as
(
select
a.*,
WIT_CREATED_DATETIME as created_datetime,
cast (WIT_STATUS_ID as string) as status_id,
cast(WIT_STATUS_DETAIL_ID as string) as status_detail_code,
'Withdrawals' as pay_marketplace_id,
WIT_WITHDRAWAL_DOL_AMT as total_paid_dol_amt,
'Withdrawals' as PRODUCT_TYPE,
'MO' as FLOW_TYPE,
'No aplica' as vertical_frenado,
0 as aplica_CBK,
'No aplica' as metodo_pago,
0 as tarjeta
from `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ` a
inner join (select WIT_WITHDRAWAL_ID, WIT_WITHDRAWAL_DOL_AMT,WIT_CREATED_DATETIME,WIT_STATUS_ID,WIT_STATUS_DETAIL_ID
from `meli-bi-data.WHOWNER.BT_MP_WITHDRAWALS` where cast(WIT_CREATED_DATETIME as date) >='2024-01-01') c on c.WIT_WITHDRAWAL_ID = a.operation_id
where 1=1
and c.WIT_WITHDRAWAL_ID is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY gca_id,operation_id ORDER BY fecha_apertura_caso DESC) <= 1
);


create or replace table `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ_final`
PARTITION BY date(fecha_apertura_caso) as
(
select a.*,bcd.created_datetime, bcd.status_id, bcd.status_detail_code, bcd.pay_marketplace_id, bcd.total_paid_dol_amt, bcd.PRODUCT_TYPE, bcd.FLOW_TYPE, bcd.vertical_frenado, bcd.aplica_CBK, bcd.metodo_pago, bcd.tarjeta from `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ` a
left join (select b.* from `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ_payments` b UNION ALL select c.* from `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ_withdrawals` c UNION ALL select d.* from `SBOX_PFFINTECHATO.desconocimiento_ops_FAQ_retiros` d) bcd on bcd.operation_id = a.operation_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY gca_id,operation_id ORDER BY fecha_apertura_caso DESC) <= 1
);