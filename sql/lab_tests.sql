with lab_tests as (
    select hadm_id, charttime,
           case when itemid in (51300,51301) then 'wbc'
                when itemid in (50811,51222) then 'hemoglobin'
                when itemid in (51265) then 'platelet'
                when itemid in (50824,50983) then 'sodium'
                when itemid in (50822,50971) then 'potassium'
                when itemid in (50882) then 'bicarbonate'
                when itemid in (50806,50902) then 'chloride'
                when itemid in (51006) then 'bun'
                when itemid in (50813) then 'lactate'
                when itemid in (50912) then 'creatinine'
                when itemid in (50820) then 'ph'
                when itemid in (50821) then 'po2'
                when itemid in (50818) then 'pco2'
                when itemid in (50963) then 'bnp'
                when itemid in (51002,51003) then 'troponin'
                when itemid in (50910,50911) then 'creatinine_kinase'
           else null end as label,

           case when itemid in (50821) and valuenum > 800 then null
                when itemid in (50882) and valuenum > 10000 then null
                when itemid in (50811,51222) and valuenum > 50 then null
                when itemid in (50806,50902) and valuenum > 10000 then null
                when itemid in (50912) and valuenum > 150 then null
                when itemid in (51265) and valuenum > 10000 then null
                when itemid in (50822,50971) and valuenum > 30 then null
                when itemid in (50824,50983) and valuenum > 200 then null
                when itemid in (51006) and valuenum > 300 then null
                when itemid in (51300,51301) and valuenum > 100 then null
                when itemid in (50813) and valuenum > 50 then null
                when valuenum <= 0 then null
           else valuenum end as valuenum
    from labevents
)

, lab_tests_cohort as (
    select subject_id, hadm_id, icustay_id, charttime, label, valuenum
    from cohort
    left join lab_tests using (hadm_id)
    where charttime between intime and intime + interval '1 day'
          and charttime between intime and outtime
          and label is not null
          and valuenum is not null
)

select * from lab_tests_cohort;