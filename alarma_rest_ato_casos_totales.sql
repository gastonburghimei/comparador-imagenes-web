Select action_id,
       case_type,
       action_type,
       action_status,
       action_date,
       user_id,
       case_id,
       sit_site_id,
       admin_id,
       date_created
from `WHOWNER.BT_ACTION_MR`
where 1=1
and action_type in ('notify_rollback', 'notify_reinstatement') 
and UPPER(infraction_type) = 'ATO'
and date_created > date_sub(current_date(), interval 2 DAY)
and case_type = 'ato'
and admin_id not in ('Exp_ATO_CX', 'manual_review', 'fraudML',
      'mr-backlog-consumers-payments', 'manual-review-actions')
order by date_created, admin_id