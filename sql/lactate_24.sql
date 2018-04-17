drop materialized view if exists lactate_0_24 cascade;
create materialized view lactate_0_24 as (
    select hadm_id, valuenum, charttime
    from labevents
    where itemid = 50813
    and hadm_id is not null
    and valuenum is not null
    and valuenum > 0
);

drop materialized view if exists lactate_before_echo_24 cascade;
create materialized view lactate_before_echo_24 as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime desc) as lactate_before,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime desc) as charttime
    from merged_data co
    inner join lactate_0_24 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 1
    and sr.charttime >= co.intime
    and date_trunc('day', sr.charttime) <= co.echo_time
);

drop materialized view if exists lactate_after_echo_24 cascade;
create materialized view lactate_after_echo_24 as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime) as lactate_after,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime) as charttime
    from merged_data co
    inner join lactate_0_24 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 1
    and sr.charttime >= (co.echo_time + interval '2' day)
);

drop materialized view if exists lactate_diff_echo_24 cascade;
create materialized view lactate_diff_echo_24 as (
    select be.hadm_id, be.lactate_before - af.lactate_after as lactate_diff
    from lactate_before_echo_24 be
    inner join lactate_after_echo_24 af
    on be.hadm_id = af.hadm_id
    and lactate_before is not null
    and lactate_after is not null
);

drop materialized view if exists lactate_death_echo_24 cascade;
create materialized view lactate_death_echo_24 as (
    select hadm_id
    from merged_data
    where echo = 1
    and deathtime < (echo_time + interval '2' day)
);

drop materialized view if exists lactate_discharge_echo_24 cascade;
create materialized view lactate_discharge_echo_24 as (
    with stg_0 as (
        select *
        from merged_data co
        left join (select hadm_id, dischtime from admissions) ad using (hadm_id)
    )

    select hadm_id
    from stg_0
    where echo = 1
    and (deathtime is null or deathtime >= (echo_time + interval '2' day))
    and dischtime < (echo_time + interval '2' day)
);

drop materialized view if exists lactate_before_non_24 cascade;
create materialized view lactate_before_non_24 as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime) as lactate_before,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime) as charttime
    from merged_data co
    inner join lactate_0_24 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 0
    and sr.charttime >= co.intime
);

drop materialized view if exists lactate_after_non_24 cascade;
create materialized view lactate_after_non_24 as (
    with stg_0 as (
        select co.hadm_id, sr.lactate_before, sr.charttime, co.outtime, co.intime
        from merged_data co
        inner join lactate_before_non_24 sr
        on co.hadm_id = sr.hadm_id
    )

    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime) as lactate_after,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime) as charttime
    from stg_0 co
    inner join lactate_0_24 sr
    on co.hadm_id = sr.hadm_id
    and sr.charttime >= (co.charttime + interval '1' day)
);

drop materialized view if exists lactate_diff_non_24 cascade;
create materialized view lactate_diff_non_24 as (
    select be.hadm_id, be.lactate_before - af.lactate_after as lactate_diff
    from lactate_before_non_24 be
    inner join lactate_after_non_24 af
    on be.hadm_id = af.hadm_id
    and lactate_before is not null
    and lactate_after is not null
);

drop materialized view if exists lactate_death_non_24 cascade;
create materialized view lactate_death_non_24 as (
    select co.hadm_id
    from merged_data co
    inner join lactate_before_non_24 sr
    on co.hadm_id = sr.hadm_id
    and co.deathtime < (sr.charttime + interval '1' day)
);

drop materialized view if exists lactate_discharge_non_24 cascade;
create materialized view lactate_discharge_non_24 as (
    with stg_0 as (
        select *
        from merged_data co
        left join (select hadm_id, dischtime from admissions) ad using (hadm_id)
    )

    select co.hadm_id
    from stg_0 co
    inner join lactate_before_non_24 sr
    on co.hadm_id = sr.hadm_id
    and (co.deathtime is null or deathtime >= (sr.charttime + interval '1' day))
    and co.dischtime < (sr.charttime + interval '1' day)
);
drop materialized view if exists lactate_death_24 cascade;
create materialized view lactate_death_24 as (
    select hadm_id from lactate_death_echo_24
    union
    select hadm_id from lactate_death_non_24
);

drop materialized view if exists lactate_diff_24 cascade;
create materialized view lactate_diff_24 as (
    select hadm_id, lactate_diff from lactate_diff_echo_24
    union
    select hadm_id, lactate_diff from lactate_diff_non_24
);

drop materialized view if exists lactate_discharge_24 cascade;
create materialized view lactate_discharge_24 as (
    select hadm_id from lactate_discharge_echo_24
    union
    select hadm_id from lactate_discharge_non_24
);
