select *
from `reauth-wixgkqu5uzm-furyid.Dataset_prod.reauth_tracks_v2`
where 1=1
and DATE(_PARTITIONTIME) >= '2024-10-01' --Poner fecha desde cuando se quiere ver
and cast(created_at as date) = '2024-10-01' --Poner fecha desde cuando se quiere ver
and user_id = '39144101' --Aca poner usuario a chequear