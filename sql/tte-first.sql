with pop_1 as (
    select *
    from population pop
    left join (select icustay_id, hadm_id from icustays) icu using (icustay_id)
)

, pop_2 as (
    select *
    from pop_1
    where angus = 1
    and age >= 18
    and icu_order = 1
    and first_careunit in ('MICU', 'SICU')
)

, tte_1 as (
    select hadm_id, min(chartdate) as echo_time
    from noteevents
    where lower(category) like 'echo'
    and lower(description) like 'report'
    group by hadm_id
)

, tte_2 as (
    select pop.icustay_id, extract(day from (ec.echo_time - date_trunc('day', pop.intime))) as time_to_echo
    from pop_2 pop left join tte_1 ec using (hadm_id)
)

select * from tte_2;
