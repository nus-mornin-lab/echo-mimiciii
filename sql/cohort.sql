select *
  from population
 where angus = 1
   and age >= 18
   and icu_order = 1
   and first_careunit in ('MICU', 'SICU')
   and (echo_time is null or (echo_time <= outtime
   and echo_time >= date_trunc('day', intime)));