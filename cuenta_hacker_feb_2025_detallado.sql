SELECT 
  h.*,
  ROUND(SUM(PAY_TRANSACTION_DOL_AMT),2) as monto_recibido,
  count(p.PAY_PAYMENT_ID) as cantidad_recibida
from `meli-bi-data.SBOX_PFFINTECHATO.cta_hacker_feb` h
 inner join `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS` p
    on h.user_id = p.CUS_CUST_ID_SEL
    AND p.pay_move_date >= DATE_SUB(h.SENTENCE_DATE, INTERVAL 90 DAY)
    AND p.pay_move_date <= h.SENTENCE_DATE
    and p.pay_move_date >= '2024-11-01'
WHERE 1=1
AND p.pay_status_id = 'approved'
and p.tpv_flag = 1
group by 1,2,3,4,5,6,7,8,9,10