with icu_age as (
    select *,
           date_part('year', age(intime, dob)) as age
      from icustays
      left join patients using (subject_id)
)

-- select intime, dob, age from icu_age limit 10;

, icu_adult as (
    select icustay_id
      from icu_age
     where age >= 18
)

-- select * from icu_adult limit 10;

, icu_first as (
    select first_value(icustay_id) over (partition by subject_id order by intime) as icustay_id
      from icustays
)

-- select * from icu_first limit 10;

, icu_flt_id as (
    select subject_id, hadm_id, icustay_id
      from icustays
      join icu_adult using (icustay_id)
      join icu_first using (icustay_id)
)

-- select * from icu_flt_id limit 10; -- 53362

, population as (
    select subject_id, hadm_id, icustay_id, angus
      from icu_flt_id
      left join angus_sepsis using (subject_id, hadm_id)
)

-- select * from population;
-- select count(*) from population where angus = 1; -- 16125

, echo_group as (
    select distinct hadm_id,
           first_value(chartdate) over (partition by hadm_id order by chartdate) as echo_time
      from noteevents
     where lower(category) like 'echo'
       and lower(description) like 'report'
)

-- select * from echo_group limit 10;

, cohort as (
    select *
      from population
      left join echo_group using (hadm_id)
)

select * from cohort;