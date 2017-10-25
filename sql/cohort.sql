select icustay_id, echo_time
  from population
 where angus = 1
   and age >= 18
   and icu_order = 1
   and first_careunit in ('MICU', 'SICU');