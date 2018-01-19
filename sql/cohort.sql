select *,
       case when echo_time is null then 0 else 1 end as echo
  from (select subject_id, hadm_id, icustay_id
          from icustays) icu
 right join population using (icustay_id)
 where angus = 1
   and age >= 18
   and icu_order = 1
   and first_careunit in ('MICU', 'SICU')
   and (echo_time is null or (echo_time <= outtime
   and echo_time >= date_trunc('day', intime)));
