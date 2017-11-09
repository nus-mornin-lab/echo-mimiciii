with lab_tests_taken as (
    select hadm_id,
           coalesce(max(case when label = 'hemoglobin' then 1 else null end), 0) as hemoglobin,
           coalesce(max(case when label = 'wbc' then 1 else null end), 0) as wbc,
           coalesce(max(case when label = 'lactate' then 1 else null end), 0) as lactate,
           coalesce(max(case when label = 'po2' then 1 else null end), 0) as po2,
           coalesce(max(case when label = 'troponin' then 1 else null end), 0) as troponin,
           coalesce(max(case when label = 'potassium' then 1 else null end), 0) as potassium,
           coalesce(max(case when label = 'creatinine_kinase' then 1 else null end), 0) as creatinine_kinase,
           coalesce(max(case when label = 'creatinine' then 1 else null end), 0) as creatinine,
           coalesce(max(case when label = 'pco2' then 1 else null end), 0) as pco2,
           coalesce(max(case when label = 'bnp' then 1 else null end), 0) as bnp,
           coalesce(max(case when label = 'bun' then 1 else null end), 0) as bun,
           coalesce(max(case when label = 'bicarbonate' then 1 else null end), 0) as bicarbonate,
           coalesce(max(case when label = 'platelet' then 1 else null end), 0) as platelet,
           coalesce(max(case when label = 'sodium' then 1 else null end), 0) as sodium,
           coalesce(max(case when label = 'chloride' then 1 else null end), 0) as chloride,
           coalesce(max(case when label = 'ph' then 1 else null end), 0) as ph
    from lab_tests
    group by subject_id, hadm_id, icustay_id
)

select * from lab_tests_taken;