drop materialized view if exists creatinine_0 cascade;
create materialized view creatinine_0 as (
    select hadm_id, valuenum, charttime
    from labevents
    where itemid = 50912
    and hadm_id is not null
    and valuenum is not null
    and valuenum > 0
);

drop materialized view if exists creatinine_before_echo cascade;
create materialized view creatinine_before_echo as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime desc) as creatinine_before,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime desc) as charttime
    from merged_data co
    inner join creatinine_0 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 1
    and sr.charttime >= co.intime
    and date_trunc('day', sr.charttime) <= co.echo_time
);

drop materialized view if exists creatinine_after_echo cascade;
create materialized view creatinine_after_echo as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime) as creatinine_after,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime) as charttime
    from merged_data co
    inner join creatinine_0 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 1
    and sr.charttime >= (co.echo_time + interval '3' day)
);

drop materialized view if exists creatinine_diff_echo cascade;
create materialized view creatinine_diff_echo as (
    select be.hadm_id, be.creatinine_before - af.creatinine_after as creatinine_diff
    from creatinine_before_echo be
    inner join creatinine_after_echo af
    on be.hadm_id = af.hadm_id
    and creatinine_before is not null
    and creatinine_after is not null
);

drop materialized view if exists creatinine_death_echo cascade;
create materialized view creatinine_death_echo as (
    select hadm_id
    from merged_data
    where echo = 1
    and deathtime < (echo_time + interval '3' day)
);

drop materialized view if exists creatinine_discharge_echo cascade;
create materialized view creatinine_discharge_echo as (
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
