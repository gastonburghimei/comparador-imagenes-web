-- DELETE ATO_BQ
DELETE FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
WHERE op_dt>= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
;
-- Paso 10 -> Creación ATO_BQ

INSERT INTO  `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` 
    SELECT
        con.* except(config_id_ctx,flow_type_ctx,ref_id_nw_dev,device_id,device_type,device_creation_date_all_users, device_creation_date,ip,face_val_30m, tipo_login, action_date, action_id)
        ,case 
            when mar.action_type = 'hacker_fraud' then 'ATO'
            when mar.action_type = 'device_theft' then 'DT'
            else null
        end as tipo_robo
        ,rob.min_op_dt
        ,rob.max_op_dt
        -- ,pat.saldo_max_previo
        -- ,pat.usuario_patot
        ,kyc.KYC_ENTITY_TYPE as kyc_entity_type
        ,kyc.KYC_KNOWLEDGE_LEVEL as kyc_knowledge_level
        ,seg.CUST_SEGMENT_CROSS as cust_segment_cross
        ,seg.CUST_SUB_SEGMENT_CROSS as cust_sub_segment_cross
        -- ,stc.sc_cust_id as scoring_id
        -- ,stc.cust_id as scoring_id
        ,stc.cust_id as scoring_id
        -- ,stc.user_id as scoring_id
        ,stc.profile_id as profile_id
        -- ,stc.exec_time
        -- ,stc.version
        ,stc.provider_id
        ,stc.industry
        ,stc.points
        ,stc.risk
        ,stc.created_date as stc_created_date
        ,stc.config_id as config_id_stc,
        con.config_id_ctx,
        con.flow_type_ctx,
        con.ref_id_nw_dev,
        case 
          when con.flow_type = 'MC' then devmc.device_id
          else con.device_id
        end as device_id,
        
        case 
            when con.flow_type = 'MC' and devmc.browser_type IS NULL and devmc.os IN ('android', 'iOS') then 'mobile nativo'
            when con.flow_type = 'MC' and devmc.browser_type = 'Desktop' and devmc.os IN ('Windows', 'Linux', 'MacOS') then 'desktop'
            when con.flow_type = 'MC' and devmc.browser_type = 'Mobile' and devmc.os IN ('Linux', 'iOS') then 'web mobile'
            else con.device_type
        end as device_type,
        
        case 
          when con.flow_type = 'MC' then devmc.device_creation_date
          else con.device_creation_date_all_users
        end as device_creation_date_all_users,
        
        case
          when con.flow_type = 'MC' then devmc.creation_date 
          else con.device_creation_date
          end as device_creation_date,
          
        case
          when con.flow_type = 'MC' then devmc.ip 
          else con.ip
          end as ip

        -- ,ctx.config_id as config_id_ctx
        -- ,ctx.flow_type as flow_type_ctx
        -- ,dev.spmt_ref_id_nw as ref_id_nw_dev
        -- ,dev.device_id
        -- ,case when dev.browser_type IS NULL and dev.os IN ('android', 'iOS') then 'mobile nativo'
        --     when dev.browser_type = 'Desktop' and dev.os IN ('Windows', 'Linux', 'MacOS') then 'desktop'
        --     when dev.browser_type = 'Mobile' and dev.os IN ('Linux', 'iOS') then 'web mobile'
        --     else 'unkown'
        -- end as device_type
        -- ,dev.device_creation_date as device_creation_date_all_users
        -- ,dev.creation_date as device_creation_date
        -- ,dev.ip
        --,ROUND(timestamp_diff(CAST(op_datetime AS TIMESTAMP), CAST(dev.creation_date AS TIMESTAMP), minute)/60,2) device_hour_age
        --,ROUND((timestamp_diff(CAST(op_datetime AS TIMESTAMP), CAST(dev.creation_date AS TIMESTAMP), minute)/60)/24,2) device_day_age
        --,ROUND((timestamp_diff(CAST(op_datetime AS TIMESTAMP), CAST(dev.creation_date AS TIMESTAMP), minute)/60)/24/30,2) device_month_age
        --,dev.popular_apps_count
        --,dev.popular_old_apps_count
        ,prio.prioridad_final
        ,prio.red_social_final as red_social
        --,case
            --when prio.prioridad_final = 'MALWARE' then 'Troyano Bancario'
            --when m.origen = 'MALWARE' then 'Troyano Bancario'
            --when c.origen = 'Cookies' then 'Cookies'
            --when prio.prioridad_final = 'ROBO_DV' then 'Robo Dispositivo'
            /*when h.origen = 'PHISHING' then 'Phishing'*/
            --when prio.prioridad_final = 'PHISHING' then 'Phishing'
            --when r.comment_remoto = 1 then 'Acceso Remoto'
            --when f.origen = 'INFOSTEALERS CREDENCIALES' and s.Ato_posterior = 1 then 'Infostealers Credenciales'
            --when prio.prioridad_final = 'ING_SOCIAL_llamado_Wapp' then 'Ingeniería Social'
            --when prio.prioridad_final = 'ING_SOCIAL_redsocial' then 'Ingeniería Social'
            --when prio.prioridad_final = 'ING_SOCIAL_sinespecificar' then 'Ingeniería Social'
            --else prio.prioridad_final
        --end as origen_final
        --,case
            --when prio.prioridad_final = 'MALWARE' then 'Troyano Bancario faq'
            --when m.origen = 'MALWARE' then 'Troyano Bancario Hades'
            --when c.origen = 'Cookies' then 'Cookies'
            --when prio.prioridad_final = 'ROBO_DV' then 'Robo Dispositivo'
            /*when h.origen = 'PHISHING' then 'Phishing Hellfish'
            when prio.prioridad_final = 'PHISHING' then 'Phishing faq'*/
            --when r.comment_remoto = 1 then 'Acceso Remoto'
            --when f.origen = 'INFOSTEALERS CREDENCIALES' and s.Ato_posterior = 1 then 'Infostealers Credenciales'
            --when prio.prioridad_final = 'ING_SOCIAL_llamado_Wapp' then 'Ingeniería Social'
            --when prio.prioridad_final = 'ING_SOCIAL_redsocial' then 'Ingeniería Social'
            --when prio.prioridad_final = 'ING_SOCIAL_sinespecificar' then 'Ingeniería Social'
            --else prio.prioridad_final
        --end as origen_subfinal
        -- ,p.casuistica as casuisticas
        ,case 
            when (mar.action_type = 'device_theft' and con.flow_type IN ('MT','MO','PO','MI','MC')) then 1-- and (pat.usuario_patot = 0 or pat.usuario_patot = 1)) then 1
  	        when (mar.action_type = 'hacker_fraud' and con.flow_type IN ('MT','MO','PO')) then 1-- and pat.usuario_patot = 1) then 1
  	        when (mar.action_type = 'hacker_fraud' and con.flow_type IN ('MC', 'MI')) then 1-- and pat.usuario_patot = 0) then 1
	        else 0
	     end as reporte
        /*,tier1.since_Tier1
        ,tier2.since_Tier2
        ,case 
            when (op_dt <= since_Tier1 or (since_Tier1 is null and since_Tier2 is null)) then 'Tier 1'
            when (op_dt > since_Tier1 and (op_dt <= since_Tier2 or since_Tier2 is null)) then 'Tier 1'
            when op_dt < since_Tier2 and since_Tier1 is null then 'Tier 1'
            when op_dt > since_Tier2 then 'Tier 2'
          else null
        end as Tier_ATO*/
        /*,case 
            when ti.last_tier_status = 1 then tier
            else null
        end as Tier_ATO*/
        ,case
            when ti.tier = 1 then 'Tier 1'
            when ti.tier = 2 then 'Tier 2'
          else null
        end as Tier_ATO
        ,case
            when tier.policy_tier = 'Tier1' then 'Tier 1'
            when tier.policy_tier = 'Tier2' then 'Tier 2'
          else null
        end as Policy_Tier
        ,con.face_val_30m
        ,case
            when des.contramarca_final >= 1 then 1
            else 0
        end as marca_automatica
        ,con.tipo_login
        ,con.action_date
        ,con.action_id
    FROM `meli-bi-data.SBOX_PFFINTECHATO.consolidado_tmp` con
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robos_min_max` rob
            on con.cust_id = rob.cust_id
        -- LEFT JOIN robos_patot pat
            -- on con.cust_id = pat.cust_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.marcas_hacker_tmp` mar
            on con.action_field_value = mar.action_field_value
            and con.operation_id = mar.action_value
        LEFT JOIN -- `meli-bi-data.WHOWNER.BT_MP_SCORING_TO_CUST` stc
        -- pre_scoring stc-- `warehouse-cross-pf.refined.models_risk_scoring`stc
        `warehouse-cross-pf.scoring.scoring_to_cust` stc
            on con.operation_id = stc.ref_id_nw
            and stc.flow_type in ('PO','MI','MT','MF','MC','MO')
            and con.flow_type = stc.flow_type
            and stc.created_date >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN `warehouse-cross-pf.scoring.context` ctx
            -- on stc.sc_cust_id = ctx.execution_id -- CAMBIO DE LA SCORING: ctx.scoring_id
            -- on stc.cust_id = ctx.execution_id -- Raro
           --  on stc.ref_id_nw = ctx.ref_id_nw
          --   and cast(ctx.created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        -- LEFT JOIN `warehouse-cross-pf.scoring.device_ml` dev
            -- on stc.ref_id_nw = dev.spmt_ref_id_nw
            -- on stc.cust_id = dev.execution_id -- CAMBIO DE LA SCORING: dev.scoring_id
           --  on stc.ref_id_nw = dev.spmt_ref_id_nw
            -- and cast(dev.spmt_created_date as date) >= DATE_SUB(DATE_SUB(current_date, interval 6 month), INTERVAL (EXTRACT(DAY FROM current_date)-1) DAY)
        LEFT JOIN `meli-bi-data.WHOWNER.LK_KYC_VAULT_USER` kyc
            on con.cust_id = kyc.cus_cust_id
        -- Cruzo con la prioridad
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.regla_prioridad_tmp` prio
            on con.cust_id = CAST (prio.gca_cust_id as numeric)
        -- Agrego lo de Tiers
        /*LEFT JOIN tier1
            on con.cust_id = tier1.user_id
        LEFT JOIN tier2
            on con.cust_id = tier2.user_id  */
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.tiers_ato` ti
            on con.cust_id = ti.user_id
        -- Lo nuevo de Tiers 10/4
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.policy_tier` tier
            on con.cust_id = tier.user_id
        -- Agrego si paso Face Valid 3/03
         LEFT JOIN `warehouse-cross-pf.scoring_mc.device_ml` devmc
            on con.operation_id = devmc.spmt_ref_id_nw
        LEFT JOIN `warehouse-cross-pf.scoring_po.reauth_status` re
            on re.spmt_ref_id_nw = con.operation_id 
            and re.spmt_created_date >= '2023-06-01' 
            and re.spmt_flow_type = stc.flow_type
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.desconocimientos_total` des
            on con.operation_id = des.operation_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Foresee_Pablo` f
            on con.cust_id = f.user_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.saldos_foresee` s
            on con.cust_id = s.user_id
        /*LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Hellfish_Pablo` h
            on con.cust_id = h.user_id*/
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.ato_remoto` r
            on con.cust_id = r.cust_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.Malware_Pablo` m
            on con.cust_id = m.user_id
        LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.robo_cookies` c
            on con.cust_id = c.cust_id
            and con.operation_id = c.ref_id_nw
        /*LEFT JOIN `meli-bi-data.SBOX_PFFINTECHATO.perfil_users_tmp` p
            on con.cust_id = p.GCA_CUST_ID*/
        LEFT JOIN (
            SELECT
                CUS_CUST_ID,
                CUST_SEGMENT_CROSS,
                CUST_SUB_SEGMENT_CROSS
            FROM `meli-bi-data.WHOWNER.LK_MP_SEGMENTATION_SELLERS`
            QUALIFY 1 = row_number () OVER (PARTITION BY CUS_CUST_ID ORDER BY TIM_MONTH DESC)
        ) seg
            on con.cust_id = seg.cus_cust_id
    WHERE 1 = 1
    QUALIFY 1 = row_number () OVER (PARTITION BY operation_id ORDER BY op_amt DESC) 
;

-- 24/11
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET prioridad_final = 'indefinido' where prioridad_final is null
;

--UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
--SET origen_final = 'indefinido' where origen_final is null
--;

--UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
--SET origen_subfinal = 'indefinido' where origen_subfinal is null
--;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET prioridad_final = 'ROBO_DV' where tipo_robo = 'DT'
;

--UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
--SET origen_final = 'ROBO_DV' where tipo_robo = 'DT'
--;

--UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
--SET origen_subfinal = 'ROBO_DV' where tipo_robo = 'DT'
--;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_segment_cross = 'Sellers Mktpl' where cust_segment_cross = 'NA'
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_sub_segment_cross = 'Sellers Mktpl' where cust_sub_segment_cross = 'NA'
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_segment_cross = 'Payers' where cust_segment_cross is null
;

UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET cust_sub_segment_cross = 'Payers' where cust_sub_segment_cross is null
;