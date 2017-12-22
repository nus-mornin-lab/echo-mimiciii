with vasofirstday as (
    select icustay_id,
           max(case when starttime between intime and intime + interval '1 day' and
               starttime <= outtime then 1 else 0 end) as vaso
    from cohort
    left join vasopressordurations using (icustay_id)
    group by icustay_id
)

, icu_adm_wday as (
    select icustay_id,
           extract(dow from intime) as icu_adm_weekday,
           extract(hour from intime) as icu_adm_hour
    from cohort
)

, mort as (
    select co.hadm_id,
           coalesce(adm.deathtime, pat.dod, null) as deathtime
    from cohort co
    left join (select hadm_id, deathtime from admissions) adm using (hadm_id)
    left join patients pat using (subject_id)
)

, mort_28 as (
    select hadm_id,
           case when deathtime <= (co.intime + interval '28' day) then 1 else 0 end as mort_28_day
    from cohort co
    natural left join mort
)

, basics as (
    select *,
           extract(epoch from (outtime - intime))/24.0/60.0/60.0 as icu_los_day
    from cohort co
    natural left join (select subject_id, gender from patients) g
    natural left join (select icustay_id, weight from weightfirstday) w
    natural left join (select icustay_id, saps from saps) sa
    natural left join (select icustay_id, sofa from sofa) so
    natural left join (select hadm_id, elixhauser_vanwalraven as elix_score
                       from elixhauser_ahrq_score) elix
    natural left join (select icustay_id, vent from ventfirstday) vent
    natural left join vasofirstday
    natural left join icu_adm_wday
    natural left join mort
    natural left join mort_28
)

select * from basics;