with norepinephrine_cv as (
    select icustay_id, amount, charttime
    from inputevents_cv
    where itemid in (30047,30120)
)

, norepinephrine_mv as (
    select icustay_id, amount, starttime, endtime
    from inputevents_mv
    where itemid in (221906)
)

, norepinephrine as (
    select co.icustay_id, coalesce(mv.amount, cv.amount, 0) as amount
    from cohort co
    left join norepinephrine_mv mv on co.icustay_id = mv.icustay_id
        and mv.starttime between co.intime and co.outtime
    left join norepinephrine_cv cv on co.icustay_id = cv.icustay_id
        and cv.charttime between co.intime and co.outtime
)

, norepinephrine_max as (
    select icustay_id, max(amount) as norepinephrine_max
    from norepinephrine
    group by icustay_id
)

, dobutamine_cv as (
    select icustay_id, amount, charttime
    from inputevents_cv
    where itemid in (30042,30306)
)

, dobutamine_mv as (
    select icustay_id, amount, starttime, endtime
    from inputevents_mv
    where itemid in (221653)
)

, dobutamine as (
    select co.icustay_id, coalesce(mv.amount, cv.amount) as amount
    from cohort co
    left join dobutamine_mv mv on co.icustay_id = mv.icustay_id
        and mv.starttime between co.intime and co.outtime
    left join dobutamine_cv cv on co.icustay_id = cv.icustay_id
        and cv.charttime between co.intime and co.outtime
)

, dobutamine_flag as (
    select icustay_id,
        case when amount is not null then 1 else 0 end as dobutamine_flag
    from dobutamine
)

-- select * from norepinephrine_max;

, vasoduration_discrete as (
    select co.icustay_id, vaso.duration_hours
    from cohort co
    left join vasopressordurations vaso on co.icustay_id = vaso.icustay_id
        and vaso.starttime between co.intime and co.outtime
        and vaso.endtime between co.intime and co.outtime
)

, vasoduration as (
    select icustay_id, coalesce(sum(duration_hours), 0) as vasoduration
    from vasoduration_discrete
    group by icustay_id
)

, ventduration_discrete as (
    select co.icustay_id, vent.duration_hours
    from cohort co
    left join ventdurations vent on co.icustay_id = vent.icustay_id
        and vent.starttime between co.intime and co.outtime
        and vent.endtime between co.intime and co.outtime
)

, ventduration as (
    select icustay_id, coalesce(sum(duration_hours), 0) as ventduration
    from ventduration_discrete
                group by icustay_id
)

, serum_1 as (
    select hadm_id, valuenum, charttime,
        case when itemid in (50813) then 'lactate'
             when itemid in (50912) then 'creatinine'
        else null end as label
    from labevents
    where itemid in (50813, 50912)
    and valuenum is not null
    and valuenum > 0
)

, serum_2 as (
    select co.hadm_id, se.label, se.valuenum, se.charttime
    from cohort co
    left join serum_1 se on co.hadm_id = se.hadm_id
        and se.charttime between co.intime and co.outtime
)

, serum_3 as (
    select distinct hadm_id, label,
        max(valuenum) over (partition by hadm_id, label) as max_val,
        first_value(valuenum) over (partition by hadm_id, label order by charttime desc) as last_val
    from serum_2
)

, serum_4 as (
    select hadm_id, label,
        max_val - last_val as reduction
    from serum_3
)

, serum as (
    select hadm_id,
        max(case when label = 'lactate' then reduction else null end) as lactate_reduction,
        max(case when label = 'creatinine' then reduction else null end) as creatinine_reduction
    from serum_4
    group by hadm_id
)

, subgroup as (
    select *
    from (select icustay_id, hadm_id from cohort)
    natural left join norepinephrine_max
    natural left join dobutamine_flag
    natural left join vasoduration
    natural left join ventduration
    natural left join serum
)

select * from subgroup;
