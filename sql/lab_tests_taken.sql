with lab_tests_taken as (
    select hadm_id,
           max(case when label = 'hemoglobin' then 1 else 0 end) as hemoglobin,
           max(case when label = 'wbc' then 1 else 0 end) as wbc,
           max(case when label = 'lactate' then 1 else 0 end) as lactate,
           max(case when label = 'po2' then 1 else 0 end) as po2,
           max(case when label = 'troponin' then 1 else 0 end) as troponin,
           max(case when label = 'potassium' then 1 else 0 end) as potassium,
           max(case when label = 'creatinine_kinase' then 1 else 0 end) as creatinine_kinase,
           max(case when label = 'creatinine' then 1 else 0 end) as creatinine,
           max(case when label = 'pco2' then 1 else 0 end) as pco2,
           max(case when label = 'bnp' then 1 else 0 end) as bnp,
           max(case when label = 'bun' then 1 else 0 end) as bun,
           max(case when label = 'bicarbonate' then 1 else 0 end) as bicarbonate,
           max(case when label = 'platelet' then 1 else 0 end) as platelet,
           max(case when label = 'sodium' then 1 else 0 end) as sodium,
           max(case when label = 'chloride' then 1 else 0 end) as chloride,
           max(case when label = 'ph' then 1 else 0 end) as ph
    from lab_tests
    group by hadm_id
)

select * from lab_tests_taken;