----- STEP 2 corrio ok
BEGIN

CREATE TEMP TABLE tiers_ante as (
    SELECT ato.cust_id,
      ti.user_id,
      profile,
      format_datetime('%Y-%m-%d %T', datetime(ti.publish_time)) AS profile_since,
      ti.tier,
      format_datetime('%Y-%m-%d %T', datetime(ti.since)) AS tier_since,
      ROW_NUMBER() OVER (PARTITION BY ato.cust_id ORDER BY ti.since DESC) AS last_tier_status
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
    LEFT JOIN `audit-tier-i64eh1rtv1v-furyid.tierAuditDataset.tierAuditTableData` ti
    -- `auth-tiers-iawyy6x53fx-furyid.tierAuditDataset.tierAuditTable` ti
      /*ON ato.cust_id = ti.user_id
      AND cast(ti.publish_time as datetime) <= ato.op_dt
    WHERE TIMESTAMP_TRUNC(since, HOUR) > TIMESTAMP("2021-01-01")*/
    ON ato.cust_id = ti.user_id
    AND TIMESTAMP_TRUNC(since, HOUR) > TIMESTAMP("2021-01-01")
    AND cast(ti.publish_time as datetime) <= ato.op_dt
    -- where user_id = 739815943
    -- where last_tier_status = 1
    ORDER BY since
);

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.tiers_ato` as(
    SELECT *
    FROM tiers_ante
    WHERE last_tier_status = 1
);

END;

----- STEP 3 corrio ok
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.cobertura_ato` as (
SELECT
CUS_CUST_ID as cust_id,
CASHBACK_ID as cashback_id,
MOV_MOV_ID as movimiento_id,
date(CBCK_DATE_CREATED) as fecha_creacion,
date(AUD_UPD_DTTM) as fecha_aprobacion,
cbck_status as status_cbck,
CBCK_AMOUNT_VALUE as monto_pesos,
CBCK_AMOUNT_VALUE_USD as monto_usd,
CBCK_CURRENCY_CODE as moneda,
CBCK_FINANCIAL_CECO as ceco,
CBCK_EXECUTED_BY as tl_carga,
CBCK_APPLICANT_TEAM as equipo_carga,
CBCK_REASON as reason_carga,
CBCK_STATUS as estado_cbck,
CASE  WHEN CBCK_CURRENCY_CODE = 'ARS' THEN 'MLA'
      WHEN CBCK_CURRENCY_CODE = 'BRL' THEN 'MLB'
      WHEN CBCK_CURRENCY_CODE = 'MXN' THEN 'MLM'
      WHEN CBCK_CURRENCY_CODE = 'UYU' THEN 'MLU' END AS Site,
CASE  WHEN  UPPER(CBCK_REASON) like ('%REGALO EXCEPCIONAL MERCADO PAGO') THEN '646032'
      WHEN  UPPER(CBCK_REASON) like ('%COBERTURA ATO') THEN '646032'
      WHEN  UPPER(CBCK_REASON) like ('%REGALO EXCEPCIONAL POR TRANSACCIONES NO RECONOCIDAS') THEN '646032'
      WHEN  UPPER(CBCK_REASON) like ('%REEMBOLSO DEVIDO A CONTA DESCONHECIDA') THEN '646032_ITO'
      WHEN  UPPER(CBCK_REASON) like ('%DEPÓSITO EXCEPCIONAL DE DINHEIRO%') THEN '646032'
      WHEN  UPPER(CBCK_REASON) like ('%CANCELACIÓN DE PRÉSTAMO%') THEN '624029'
      WHEN  UPPER(CBCK_REASON) like ('%COBERTURA PERFIL COOKIE%') THEN '624029'
      WHEN  UPPER(CBCK_REASON) like ('%REGALO POR PAGO DE PRÉSTAMO NO SOLICITADO%') THEN '624029'
      WHEN  UPPER(CBCK_REASON) like ('COBERTURA EXCEPCIONAL') THEN '624032'
END AS Cuenta,
CBCK_OPERATION_ID as operation_id_cbck
FROM `WHOWNER.BT_MP_CASHBACK`
WHERE (REGEXP_CONTAINS(UPPER(CBCK_APPLICANT_TEAM), 'PREVENCIÓN DE FRAUDE - FINTECH|CX ONE - BONIFICADOR'))
      AND (REGEXP_CONTAINS(UPPER(CBCK_REASON), 'REGALO EXCEPCIONAL MERCADO PAGO|COBERTURA ATO|REGALO EXCEPCIONAL POR TRANSACCIONES NO RECONOCIDAS|REEMBOLSO DEVIDO A CONTA DESCONHECIDA|DEPÓSITO EXCEPCIONAL DE DINHEIRO|CANCELACIÓN DE PRÉSTAMO|COBERTURA PERFIL COOKIE|REGALO POR PAGO DE PRÉSTAMO NO SOLICITADO|COBERTURA EXCEPCIONAL'))
QUALIFY 1=row_number() over (partition by CASHBACK_ID order by AUD_UPD_DTTM desc)
);

----- STEP 4 corrio ok
BEGIN

/*DECLARE days_subt INT64 DEFAULT 3; -- variar
DECLARE date_ini STRING DEFAULT "2023-01-01";

CREATE TEMP TABLE tiers_nuevo_ante as (
SELECT ato.cust_id,
    c.user_id,
    IF (ato_planning_profile != 'not_ato_planning',
        'Tier2',
        'Tier1') AS policy_tier,
    ROW_NUMBER() OVER (PARTITION BY c.user_id ORDER BY datetime DESC) AS last_login
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
LEFT JOIN `authevents-6ikh1q8l2t2-furyid.login.authentication_challenges` c
    ON ato.cust_id = c.user_id
    and ato.site_id = UPPER(c.site_id)
    and date(c.datetime) >= date(date_ini)
    and date(c.datetime) > DATE_ADD(date (ato.op_dt), INTERVAL -days_subt DAY)
    and date(c.datetime) <= date(ato.op_dt)
   
WHERE c.code = 'grant_session' -- Aseguro Login
);

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.policy_tier` as(
    SELECT *
    FROM tiers_nuevo_ante
    WHERE last_login = 1
);
*/

DECLARE date_ini STRING DEFAULT "2021-01-01";
DECLARE date_ini_ato STRING DEFAULT "2023-08-01";
DECLARE date_fin_ato STRING DEFAULT "2023-08-03";

WITH 
  
  ATO AS 
  (
    SELECT ato.operation_id,
           ato.device_id,
           ato.cust_id,
           ato.op_dt,
           ato.contramarca,
           ato.tipo_robo,
           ato.device_type             
    FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
    WHERE date(ato.op_dt) BETWEEN date(date_ini_ato) AND  date(date_fin_ato)   
    
  ),
  LOGIN_SUCCESS_EXP AS 
  (
    SELECT 
        c.user_id,
        c.site_id,
        c.device_id,        
        c.tracking_id,
        c.datetime,       
        CASE ato_planning_profile 
          WHEN 'not_ato_planning' THEN  'Tier1'
          ELSE 'Tier2'
        END AS policy_tier
    FROM `authevents-6ikh1q8l2t2-furyid.login.login_success` c           
    WHERE (c.datetime) >= date(date_ini) 
    AND c.user_id 
      IN (
        SELECT cust_id FROM ATO
      )
  
  ),
  DEVICES AS
  ( 
  SELECT browser.BRWSR_USER_ID AS USER_ID,
         browser.BRWSR_PROFILE_ID AS PROFILE_ID,
         browser.BRWSR_DEVICE_ID AS DEVICE_ID,
         browser.BRWSR_SNAPSHOT_DATE AS SNAPSHOT_DATE  
  FROM `meli-bi-data.WHOWNER.BT_FRD_BROWSER_DEVICE_NRT` browser
  WHERE 1=1
  AND BRWSR_SNAPSHOT_DATE_DT <= current_date()
  AND browser.BRWSR_USER_ID 
    IN (
       SELECT cust_id FROM ATO
    )
  UNION ALL  
  SELECT mobile.MBL_USER_ID AS USER_ID,
         mobile.MBL_PROFILE_ID AS PROFILE_ID,
         mobile.MBL_DEVICE_ID,
         mobile.MBL_SNAPSHOT_DATE_DTTM AS SNAPSHOT_DATE         
  FROM `meli-bi-data.WHOWNER.BT_FRD_MOBILE_DEVICE_NRT` mobile
  WHERE 1=1
  AND MBL_SNAPSHOT_DATE_DT <= current_date()
  AND mobile.MBL_USER_ID 
    IN (
       SELECT cust_id FROM ATO
    )  
  )
  
  

 SELECT operation_id,     
        policy_tier,
        cust_id
 FROM 
 (
 SELECT ROW_NUMBER() OVER (PARTITION BY ato.operation_id ORDER BY login.datetime DESC NULLS LAST) AS last_login_device,
        login.policy_tier,
        login.user_id,
        ato.operation_id, 
        ato.cust_id            
 FROM ATO ato
 
LEFT JOIN DEVICES device
 ON ato.device_id = device.DEVICE_ID
 AND date(ato.op_dt)>=date(device.SNAPSHOT_DATE) 

LEFT JOIN LOGIN_SUCCESS_EXP login 
  ON login.user_id = ato.cust_id
  AND  date(ato.op_dt) >=date(login.datetime)   
  AND login.device_id = device.PROFILE_ID
  AND device.device_id = ato.device_id 
  OR (login.user_id = ato.cust_id AND device.DEVICE_ID IS NULL AND ato.device_id  IS NULL AND date(ato.op_dt) >=date(login.datetime))
 )
 WHERE last_login_device =1; 
 
END;

----- STEP 5 corrio ok
BEGIN

CREATE or REPLACE TABLE `meli-bi-data.SBOX_PFFINTECHATO.contramarca_manual_ato` as(
SELECT ato.*, 
  con.wit_manual_contramarca, 
  con.pyt_manual_contramarca, 
  con.pay_manual_contramarca,
  case when con.wit_manual_contramarca = 1 then 1
      when con.pyt_manual_contramarca = 1 then 1
      when con.pay_manual_contramarca = 1 then 1
      else 0
  end as contramarca_manual
FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` ato
LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_contramarca_prueba` con
  on ato.operation_id = cast(con.operation_id as string)
  and ato.cust_id = con.cust_id
);

END;