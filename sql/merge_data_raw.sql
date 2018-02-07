select *
from basics
left join icd9 using (hadm_id)
left join vital_signs_unpivot using (icustay_id)
left join lab_unpivot using (hadm_id)
left join drugs using (icustay_id)
