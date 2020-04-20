with lab_summary as (
    select distinct hadm_id, label
    , first_value(valuenum) over (partition by hadm_id, label order by charttime) as fst_val
    , first_value(valuenum) over (partition by hadm_id, label order by valuenum) as min_val
    , first_value(valuenum) over (partition by hadm_id, label order by valuenum desc) as max_val
    , first_value(abnormal) over (partition by hadm_id, label order by abnormal desc) as abnormal
    from lab_tests
)

select hadm_id
, max(case when label = 'hemoglobin' then 1 else 0 end) as lab_hemoglobin_flag
, max(case when label = 'hemoglobin' then fst_val else null end) as lab_hemoglobin_first
, max(case when label = 'hemoglobin' then min_val else null end) as lab_hemoglobin_min
, max(case when label = 'hemoglobin' then max_val else null end) as lab_hemoglobin_max
, max(case when label = 'hemoglobin' then abnormal else null end) as lab_hemoglobin_abnormal
, max(case when label = 'platelet' then 1 else 0 end) as lab_platelet_flag
, max(case when label = 'platelet' then fst_val else null end) as lab_platelet_first
, max(case when label = 'platelet' then min_val else null end) as lab_platelet_min
, max(case when label = 'platelet' then max_val else null end) as lab_platelet_max
, max(case when label = 'platelet' then abnormal else null end) as lab_platelet_abnormal
, max(case when label = 'creatinine_kinase' then 1 else 0 end) as lab_creatinine_kinase_flag
, max(case when label = 'creatinine_kinase' then fst_val else null end) as lab_creatinine_kinase_first
, max(case when label = 'creatinine_kinase' then min_val else null end) as lab_creatinine_kinase_min
, max(case when label = 'creatinine_kinase' then max_val else null end) as lab_creatinine_kinase_max
, max(case when label = 'creatinine_kinase' then abnormal else null end) as lab_creatinine_kinase_abnormal
, max(case when label = 'wbc' then 1 else 0 end) as lab_wbc_flag
, max(case when label = 'wbc' then fst_val else null end) as lab_wbc_first
, max(case when label = 'wbc' then min_val else null end) as lab_wbc_min
, max(case when label = 'wbc' then max_val else null end) as lab_wbc_max
, max(case when label = 'wbc' then abnormal else null end) as lab_wbc_abnormal
, max(case when label = 'ph' then 1 else 0 end) as lab_ph_flag
, max(case when label = 'ph' then fst_val else null end) as lab_ph_first
, max(case when label = 'ph' then min_val else null end) as lab_ph_min
, max(case when label = 'ph' then max_val else null end) as lab_ph_max
, max(case when label = 'ph' then abnormal else null end) as lab_ph_abnormal
, max(case when label = 'chloride' then 1 else 0 end) as lab_chloride_flag
, max(case when label = 'chloride' then fst_val else null end) as lab_chloride_first
, max(case when label = 'chloride' then min_val else null end) as lab_chloride_min
, max(case when label = 'chloride' then max_val else null end) as lab_chloride_max
, max(case when label = 'chloride' then abnormal else null end) as lab_chloride_abnormal
, max(case when label = 'sodium' then 1 else 0 end) as lab_sodium_flag
, max(case when label = 'sodium' then fst_val else null end) as lab_sodium_first
, max(case when label = 'sodium' then min_val else null end) as lab_sodium_min
, max(case when label = 'sodium' then max_val else null end) as lab_sodium_max
, max(case when label = 'sodium' then abnormal else null end) as lab_sodium_abnormal
, max(case when label = 'bun' then 1 else 0 end) as lab_bun_flag
, max(case when label = 'bun' then fst_val else null end) as lab_bun_first
, max(case when label = 'bun' then min_val else null end) as lab_bun_min
, max(case when label = 'bun' then max_val else null end) as lab_bun_max
, max(case when label = 'bun' then abnormal else null end) as lab_bun_abnormal
, max(case when label = 'bicarbonate' then 1 else 0 end) as lab_bicarbonate_flag
, max(case when label = 'bicarbonate' then fst_val else null end) as lab_bicarbonate_first
, max(case when label = 'bicarbonate' then min_val else null end) as lab_bicarbonate_min
, max(case when label = 'bicarbonate' then max_val else null end) as lab_bicarbonate_max
, max(case when label = 'bicarbonate' then abnormal else null end) as lab_bicarbonate_abnormal
, max(case when label = 'bnp' then 1 else 0 end) as lab_bnp_flag
, max(case when label = 'bnp' then fst_val else null end) as lab_bnp_first
, max(case when label = 'bnp' then min_val else null end) as lab_bnp_min
, max(case when label = 'bnp' then max_val else null end) as lab_bnp_max
, max(case when label = 'bnp' then abnormal else null end) as lab_bnp_abnormal
, max(case when label = 'pco2' then 1 else 0 end) as lab_pco2_flag
, max(case when label = 'pco2' then fst_val else null end) as lab_pco2_first
, max(case when label = 'pco2' then min_val else null end) as lab_pco2_min
, max(case when label = 'pco2' then max_val else null end) as lab_pco2_max
, max(case when label = 'pco2' then abnormal else null end) as lab_pco2_abnormal
, max(case when label = 'creatinine' then 1 else 0 end) as lab_creatinine_flag
, max(case when label = 'creatinine' then fst_val else null end) as lab_creatinine_first
, max(case when label = 'creatinine' then min_val else null end) as lab_creatinine_min
, max(case when label = 'creatinine' then max_val else null end) as lab_creatinine_max
, max(case when label = 'creatinine' then abnormal else null end) as lab_creatinine_abnormal
, max(case when label = 'potassium' then 1 else 0 end) as lab_potassium_flag
, max(case when label = 'potassium' then fst_val else null end) as lab_potassium_first
, max(case when label = 'potassium' then min_val else null end) as lab_potassium_min
, max(case when label = 'potassium' then max_val else null end) as lab_potassium_max
, max(case when label = 'potassium' then abnormal else null end) as lab_potassium_abnormal
, max(case when label = 'troponin' then 1 else 0 end) as lab_troponin_flag
, max(case when label = 'troponin' then fst_val else null end) as lab_troponin_first
, max(case when label = 'troponin' then min_val else null end) as lab_troponin_min
, max(case when label = 'troponin' then max_val else null end) as lab_troponin_max
, max(case when label = 'troponin' then abnormal else null end) as lab_troponin_abnormal
, max(case when label = 'po2' then 1 else 0 end) as lab_po2_flag
, max(case when label = 'po2' then fst_val else null end) as lab_po2_first
, max(case when label = 'po2' then min_val else null end) as lab_po2_min
, max(case when label = 'po2' then max_val else null end) as lab_po2_max
, max(case when label = 'po2' then abnormal else null end) as lab_po2_abnormal
, max(case when label = 'lactate' then 1 else 0 end) as lab_lactate_flag
, max(case when label = 'lactate' then fst_val else null end) as lab_lactate_first
, max(case when label = 'lactate' then min_val else null end) as lab_lactate_min
, max(case when label = 'lactate' then max_val else null end) as lab_lactate_max
, max(case when label = 'lactate' then abnormal else null end) as lab_lactate_abnormal
from lab_summary
group by hadm_id
