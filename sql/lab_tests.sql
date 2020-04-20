with lab_tests as (
    select hadm_id, charttime, value,
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
           valuenum
    from labevents
)

, lab_tests_cohort as (
    select hadm_id, charttime, label, valuenum,
           case when label = 'wbc' and lab.valuenum between 4.5 and 10 then 0
                when label = 'hgb' and co.gender = 'M' and lab.valuenum between 13.8 and 17.2 then 0
                when label = 'hgb' and co.gender = 'F' and lab.valuenum between 12.1 and 15.1 then 0
                when label = 'platelet' and lab.valuenum between 150 and 400 then 0
                when label = 'sodium' and lab.valuenum between 135 and 145 then 0
                when label = 'potassium' and lab.valuenum between 3.7 and 5.2 then 0
                when label = 'bicarbonate' and lab.valuenum between 22 and 28 then 0
                when label = 'chloride' and lab.valuenum between 96 and 106 then 0
                when label = 'bun' and lab.valuenum between 6 and 20 then 0
                when label = 'lactate' and lab.valuenum between 0.5 and 2.2 then 0
                when label = 'creatinine' and co.gender = 'M' and lab.valuenum <= 1.3 then 0
                when label = 'creatinine' and co.gender = 'F' and lab.valuenum <= 1.1 then 0
                when label = 'ph' and lab.valuenum between 7.38 and 7.42 then 0
                when label = 'po2' and lab.valuenum between 75 and 100 then 0
                when label = 'pco2' and lab.valuenum between 35 and 45 then 0
                when label = 'bnp' and lab.valuenum <= 100 then 0
                when label = 'troponin' and lab.valuenum <= 0.1 then 0
                when label = 'creatinine_kinase' and lab.valuenum <= 120 then 0
                else 1 end as abnormal
    from basics co
    left join lab_tests lab using (hadm_id)
    where charttime between intime and intime + interval '1 day'
          and charttime between intime and outtime
          and label is not null
          and valuenum is not null
          and valuenum > 0
)

select * from lab_tests_cohort;