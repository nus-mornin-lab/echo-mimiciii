with angus_group as (
    select icustay_id, angus
      from icustays
      left join angus_sepsis using (hadm_id)
)

, icu_age as (
    select icustay_id,
           extract('epoch' from (intime - dob)) / 60.0 / 60.0 / 24.0 / 365.242 as age
      from icustays
      left join patients using (subject_id)
)

, icu_order as (
    select icustay_id,
           rank() over (partition by subject_id order by intime) as icu_order
      from icustays
)

, echo_hos as (
    select distinct hadm_id,
           first_value(chartdate) over (partition by hadm_id order by chartdate) as echo_time
      from noteevents
     where lower(category) like 'echo'
       and lower(description) like 'report'
)

, echo_icu as (
    select icustay_id, echo_time
      from icustays
      left join echo_hos using (hadm_id)
)

, population as (
    select *
      from (select distinct icustay_id, first_careunit, intime, outtime from icustays) a
      left join angus_group using (icustay_id)
      left join icu_age using (icustay_id)
      left join icu_order using (icustay_id)
      left join echo_icu using (icustay_id)
)

select * from population;