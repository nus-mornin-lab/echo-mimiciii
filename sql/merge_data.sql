select *
from basics
left join vital_signs_unpivot using (icustay_id)
left join lab_unpivot using (hadm_id)