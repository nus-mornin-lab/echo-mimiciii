
with summary as (
    select distinct icustay_id, label
    , first_value(valuenum) over (partition by icustay_id, label order by charttime) as fst_val
    , first_value(valuenum) over (partition by icustay_id, label order by valuenum) as min_val
    , first_value(valuenum) over (partition by icustay_id, label order by valuenum desc) as max_val
    from vital_signs
)

select icustay_id
, max(case when label = 'temp' then 1 else 0 end) as vs_temp_flag
, max(case when label = 'temp' then fst_val else null end) as vs_temp_first
, max(case when label = 'temp' then min_val else null end) as vs_temp_min
, max(case when label = 'temp' then max_val else null end) as vs_temp_max
, max(case when label = 'map' then 1 else 0 end) as vs_map_flag
, max(case when label = 'map' then fst_val else null end) as vs_map_first
, max(case when label = 'map' then min_val else null end) as vs_map_min
, max(case when label = 'map' then max_val else null end) as vs_map_max
, max(case when label = 'cvp' then 1 else 0 end) as vs_cvp_flag
, max(case when label = 'cvp' then fst_val else null end) as vs_cvp_first
, max(case when label = 'cvp' then min_val else null end) as vs_cvp_min
, max(case when label = 'cvp' then max_val else null end) as vs_cvp_max
, max(case when label = 'heart_rate' then 1 else 0 end) as vs_heart_rate_flag
, max(case when label = 'heart_rate' then fst_val else null end) as vs_heart_rate_first
, max(case when label = 'heart_rate' then min_val else null end) as vs_heart_rate_min
, max(case when label = 'heart_rate' then max_val else null end) as vs_heart_rate_max
from summary
group by icustay_id
