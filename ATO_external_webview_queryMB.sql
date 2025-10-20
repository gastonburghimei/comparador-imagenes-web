select A.cust_id, A.device_type, A.user_agent
 from `meli-bi-data.TMP.ATO_FACTORES_LOGIN_2023` A  
   inner join `authevents-6ikh1q8l2t2-furyid.login.authentication_challenges` L 
   on A.tracking_id = L.tracking_id 
   and L.datetime >= '2024-10-01' 
   and code = 'grant_session' 
   and A.op_datetime between '2024-10-01' and '2024-10-31' 
   and A.client_type = 'webview' 
   and A.cust_id = L.user_id
where CONTAINS_SUBSTR(JSON_EXTRACT(raw, '$.removed_elements'),'face')
;