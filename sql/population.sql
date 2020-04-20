with angus_group as (
    select icustay_id, angus
    from icustays
    left join angus_sepsis using (hadm_id)
)

, icu_age_raw as (
    select icustay_id,
        extract('epoch' from (intime - dob)) / 60.0 / 60.0 / 24.0 / 365.242 as age
    from icustays
    left join patients using (subject_id)
)

, icu_age as (
    select icustay_id,
        case when age >= 130 then 91.5 else age end as age
    from icu_age_raw
)

, icu_order as (
    select icustay_id,
        rank() over (partition by subject_id order by intime) as icu_order
    from icustays
)

, echo as (
    select hadm_id, chartdate as echo_time
    from noteevents
    where lower(category) like 'echo'
    and lower(description) like 'report'
)

, echo_exclude as (
    select icustay_id,
        case when bool_and(echo_time < date_trunc('day', intime - interval '1' day) or echo_time > outtime) then 1 else 0 end as echo_exclude
    from icustays left join echo using (hadm_id)
    group by icustay_id
)

, echo_include as (
    select icustay_id,
        case when bool_and(echo_time is null)
        or bool_or(echo_time between date_trunc('day', intime - interval '1' day) and outtime) then 1 else 0 end as echo_include
    from icustays left join echo using (hadm_id)
    group by icustay_id
)

, echo_first as (
    select icustay_id, min(echo_time) as echo_time
    from icustays left join echo ec using (hadm_id)
    where echo_time between date_trunc('day', intime - interval '1' day) and outtime
    group by icustay_id
)

, population as (
    select *
    from (select distinct icustay_id, first_careunit, intime, outtime from icustays) a
    left join angus_group using (icustay_id)
    left join icu_age using (icustay_id)
    left join icu_order using (icustay_id)
    left join echo_first using (icustay_id)
    left join echo_exclude using (icustay_id)
    left join echo_include using (icustay_id)
)

select * from population;
