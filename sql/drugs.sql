with drug_cv as (
    select icustay_id, charttime,
           case when itemid in (30124,30150,30308,30118,30149,30131) then 'sedative' else null end as label
    from inputevents_cv
)

, drug_mv as (
    select icustay_id, starttime, endtime,
           case when itemid in (221668,221744,225972,225942,222168) then 'sedative' else null end as label
    from inputevents_mv
)

, drug as (
    select co.icustay_id, coalesce(dm.label, dc.label) as label
    from cohort co
    left join drug_cv dc on dc.icustay_id = co.icustay_id
        and dc.label is not null
        and dc.charttime between co.intime and co.outtime
        and dc.charttime between co.intime and co.intime + interval '1' day
    left join drug_mv dm on dm.icustay_id = co.icustay_id
        and dm.label is not null
        and dm.starttime <= co.outtime
        and dm.starttime <= co.intime + interval '1' day
        and dm.endtime >= co.intime
)

, drug_unpivot as (
    select icustay_id,
        max(case when label = 'sedative' then 1 else 0 end) as sedative
    from drug
    group by icustay_id
)

select * from drug_unpivot;