with fluid_cv_0 as (
    select icustay_id, itemid, amount, amountuom, charttime
    from inputevents_cv
    where itemid in (30058,30065,
        30013,
        30187,30016,30317,30318,
        30018,30296,30190,
        30021,
        30020,30160,30159,
        30143,30061,
        30015,30060,
        30030,
        30008,
        30032,30196,30096,30197,30198,30199,30200,30325,30194,30313,30301,30191,30193,30192,30323,30314,30203,
        30099,
        30208)
    and amountuom ~* 'ml'
    and amount > 0
)

, fluid_mv_0 as (
    select icustay_id, itemid,
        case when amountuom similar to '(l|L)' then amount * 1000 else amount end as amount,
        amountuom, starttime, endtime
    from inputevents_mv
    where itemid in (225944,
        220949,
        220952,220950,
        225158,
        225943,226089,
        225828,227533,
        225159,
        225823,225825,225827,225941,225823,
        225161,
        220995,
        220862,220864,
        225916,225917,225948,225947,
        225920,
        225969)
    and amountuom ~* 'ml|l'
    and starttime <= endtime
    and amount > 0
)

, days as (
    (select icustay_id, intime as daystart,
            intime + interval '1' day as dayend,
            1 as nday
        from merged_data
        where deathtime is null or deathtime >= intime + interval '3' day)
    union
    (select icustay_id, intime + interval '1' day as daystart,
            intime + interval '2' day as dayend,
            2 as nday
        from merged_data
        where deathtime is null or deathtime >= intime + interval '3' day)
    union
    (select icustay_id, intime + interval '2' day as daystart,
            intime + interval '3' day as dayend,
            3 as nday
        from merged_data
        where deathtime is null or deathtime >= intime + interval '3' day)
)

, fluid_mv_1 as (
    select icustay_id, itemid, nday, amount, starttime, endtime, daystart, dayend,
        least(greatest(daystart, starttime), endtime) as overlapstart,
        greatest(least(dayend, endtime), starttime) as overlapend
    from days co
    inner join fluid_mv_0 mv using (icustay_id)
)

, fluid_mv_2 as (
    select icustay_id, itemid, nday,
        amount as amountfull,
        case when starttime = endtime and (starttime >= daystart and starttime < dayend) then amount
            when starttime = endtime and not (starttime >= daystart and starttime < dayend) then 0
            else 1.0 * amount * extract (epoch from (overlapend - overlapstart)) / extract (epoch from (endtime - starttime)) end as amount,
        starttime, endtime, daystart, dayend, overlapstart, overlapend
    from fluid_mv_1
)

, fluid_mv_3 as (
    select icustay_id, nday, sum(amount) as amount
    from fluid_mv_2
    group by icustay_id, nday
)

, fluid_cv_1 as (
    select icustay_id, itemid, nday, amount, charttime, daystart, dayend
    from days co
    inner join fluid_cv_0 using (icustay_id)
)

, fluid_cv_2 as (
    select icustay_id, itemid, nday,
        case when charttime between daystart and dayend then amount else null end as amount
    from fluid_cv_1
)

, fluid_cv_3 as (
    select icustay_id, nday, sum(amount) as amount
    from fluid_cv_2
    group by icustay_id, nday
)

, fluid_0 as (
    select * from fluid_mv_3
    union
    select * from fluid_cv_3
)

, fluid_1 as (
    select icustay_id, nday, sum(amount) as amount
    from fluid_0
    group by icustay_id, nday
)

select * from fluid_1;









