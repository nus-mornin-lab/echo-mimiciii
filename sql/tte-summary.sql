with tte_1 as (
    select icu.icustay_id, ne.chartdate as echo_time
    from icustays icu left join noteevents ne using (hadm_id)
    where lower(ne.category) like 'echo'
    and lower(ne.description) like 'report'
    and (ne.chartdate between (date_trunc('day', icu.intime - interval '1' day)) and icu.outtime)
)

, tte_2 as (
    select icustay_id, count(*) as echo_times
    from tte_1
    group by icustay_id
)

select icustay_id, coalesce(echo_times, 0) as echo_times
from merged_data left join tte_2 using (icustay_id);
