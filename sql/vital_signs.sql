with vital_signs as (
    select icustay_id,
           charttime,
           case when itemid in (456,52,6702,443,220052,220181,225312) then 'map'
                when itemid in (223762,676,223761,678) then 'temp'
                when itemid in (211,220045) then 'heart_rate'
                when itemid in (113,1103,220074) then 'cvp'
            else null end as label,

            case when itemid in (223761,678) and ((valuenum-32)/1.8)<10 then null
                 when itemid in (456,52,6702,443,220052,220181,225312) and (valuenum <= 0 or valuenum >= 300) then null
                 when itemid in (211,220045) and (valuenum <= 0 or valuenum >= 300) then null
                 when itemid in (223762,676) and valuenum < 10 then null
                 -- convert F to C
                 when itemid in (223761,678) then (valuenum-32)/1.8
                 -- sanity checks on data - one outliter with spo2 < 25
                 when itemid in (646,220277) and valuenum <= 25 then null
            else valuenum end as valuenum
    from chartevents
    where error is distinct from 1
)

, vital_signs_cohort as (
    select icustay_id, charttime, label, valuenum
    from cohort
    left join vital_signs using (icustay_id)
    where charttime between intime and intime + interval '1 day'
          and charttime between intime and outtime
          and label is not null
          and valuenum is not null
)

select * from vital_signs_cohort;