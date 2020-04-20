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
        case when sum(amount) is not null then 1 else 0 end as dobutamine_flag
    from dobutamine
    group by icustay_id
)

, vasofree_0 as (
    select icustay_id, starttime, 
        case when (co.intime + interval '28' day) <= endtime then (co.intime + interval '28' day) else endtime end as endtime,
        co.outtime
    from merged_data co
    left join vasopressordurations vs using (icustay_id)
)

, vasofree_1 as (
    select icustay_id, starttime,
        case when outtime <= endtime then outtime else endtime end as endtime
    from vasofree_0
)

, vasofree_2 as (
    select icustay_id, extract(epoch from endtime - starttime) / 60.0 / 60.0 as duration_hours
    from vasofree_1
)

, vasofree_3 as (
    select icustay_id, sum(duration_hours) / 24.0 as vasoduration
    from vasofree_2
    group by icustay_id
)

, vasofree_4 as (
    select icustay_id, 28 - vasoduration as vasofreeday28
    from vasofree_3
)

, vasofree as (
    select icustay_id,
        coalesce(case when mort_28_day = 0 then vasofreeday28
                      else 0 end, 28) as vasofreeday28
    from merged_data co
    left join vasofree_4 using (icustay_id)
)

, ventfree_0 as (
    select icustay_id, starttime,
        case when (co.intime + interval '28' day) <= endtime then (co.intime + interval '28' day) else endtime end as endtime,
        co.outtime
    from merged_data co
    left join ventdurations ve using (icustay_id)
)

, ventfree_1 as (
    select icustay_id, starttime,
        case when outtime <= endtime then outtime else endtime end as endtime
    from ventfree_0
)

, ventfree_2 as (
    select icustay_id, extract(epoch from endtime - starttime) / 60.0 / 60.0 as duration_hours
    from ventfree_1
)

, ventfree_3 as (
    select icustay_id, sum(duration_hours) / 24.0 as ventduration
    from ventfree_2
    group by icustay_id
)

, ventfree_4 as (
    select icustay_id, 28 - ventduration as ventfreeday28
    from ventfree_3
)

, ventfree as (
    select icustay_id,
        coalesce(case when mort_28_day = 0 then ventfreeday28
                      else 0 end, 28) as ventfreeday28
    from merged_data co
    left join ventfree_4 using (icustay_id)
)

, sofa_2 as (
    select co.icustay_id,
        case when co.deathtime between (co.intime + interval '1' day) and (co.intime + interval '2' day) then 24
        else sf.sofa end as sofa
    from merged_data co
    left join sofasecond sf using (icustay_id)
)

, sofa_3 as (
    select co.icustay_id,
        case when co.deathtime between (co.intime + interval '2' day) and (co.intime + interval '3' day) then 24
        else sf.sofa end as sofa
    from merged_data co
    left join sofathird sf using (icustay_id)
)

, sofa_3_days as (
    select *
    from (select icustay_id from cohort) co
    natural left join (select icustay_id, sofa as sofa_1 from sofa) s1
    natural left join (select icustay_id, sofa as sofa_2 from sofa_2) s2
    natural left join (select icustay_id, sofa as sofa_3 from sofa_3) s3
)

, sofa_drop as (
    select icustay_id, sofa_1 as sofa, sofa_1 - sofa_2 as sofa_drop_2, sofa_1 - sofa_3 as sofa_drop_3
    from sofa_3_days
)

, subgroup as (
    select *
    from (select icustay_id, hadm_id, echo from cohort) co
    natural left join norepinephrine_max
    natural left join dobutamine_flag
    natural left join vasofree
    natural left join ventfree
    natural left join sofa_drop
)

select * from subgroup;
