drop materialized view if exists lactate_0 cascade;
create materialized view lactate_0 as (
    select hadm_id, valuenum, charttime
    from labevents
    where itemid = 50813
    and hadm_id is not null
    and valuenum is not null
    and valuenum > 0
);

drop materialized view if exists lactate_before_echo cascade;
create materialized view lactate_before_echo as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime desc) as lactate_before,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime desc) as charttime
    from merged_data co
    inner join lactate_0 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 1
    and sr.charttime >= co.intime
    and date_trunc('day', sr.charttime) <= co.echo_time
);

drop materialized view if exists lactate_after_echo cascade;
create materialized view lactate_after_echo as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime) as lactate_after,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime) as charttime
    from merged_data co
    inner join lactate_0 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 1
    and sr.charttime >= (co.echo_time + interval '3' day)
);

drop materialized view if exists lactate_diff_echo cascade;
create materialized view lactate_diff_echo as (
    select be.hadm_id, be.lactate_before - af.lactate_after as lactate_diff
    from lactate_before_echo be
    inner join lactate_after_echo af
    on be.hadm_id = af.hadm_id
    and lactate_before is not null
    and lactate_after is not null
);

drop materialized view if exists lactate_death_echo cascade;
create materialized view lactate_death_echo as (
    select hadm_id
    from merged_data
    where echo = 1
    and deathtime < (echo_time + interval '3' day)
);

drop materialized view if exists lactate_discharge_echo cascade;
create materialized view lactate_discharge_echo as (
    with stg_0 as (
        select *
        from merged_data co
        left join (select hadm_id, dischtime from admissions) ad using (hadm_id)
    )

    select hadm_id
    from stg_0
    where echo = 1
    and (deathtime is null or deathtime >= (echo_time + interval '3' day))
    and dischtime < (echo_time + interval '3' day)
);
