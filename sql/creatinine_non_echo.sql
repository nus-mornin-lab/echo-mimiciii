drop materialized view if exists creatinine_before_non cascade;
create materialized view creatinine_before_non as (
    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime) as creatinine_before,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime) as charttime
    from merged_data co
    inner join creatinine_0 sr
    on co.hadm_id = sr.hadm_id
    and co.echo = 0
    and sr.charttime >= co.intime
);

drop materialized view if exists creatinine_after_non cascade;
create materialized view creatinine_after_non as (
    with stg_0 as (
        select co.hadm_id, sr.creatinine_before, sr.charttime, co.outtime, co.intime
        from merged_data co
        inner join creatinine_before_non sr
        on co.hadm_id = sr.hadm_id
    )

    select distinct co.hadm_id,
        first_value(sr.valuenum) over (partition by co.hadm_id order by sr.charttime) as creatinine_after,
        first_value(sr.charttime) over (partition by co.hadm_id order by sr.charttime) as charttime
    from stg_0 co
    inner join creatinine_0 sr
    on co.hadm_id = sr.hadm_id
    and sr.charttime >= (co.charttime + interval '2' day)
);

drop materialized view if exists creatinine_diff_non cascade;
create materialized view creatinine_diff_non as (
    select be.hadm_id, be.creatinine_before - af.creatinine_after as creatinine_diff
    from creatinine_before_non be
    inner join creatinine_after_non af
    on be.hadm_id = af.hadm_id
    and creatinine_before is not null
    and creatinine_after is not null
);

drop materialized view if exists creatinine_death_non cascade;
create materialized view creatinine_death_non as (
    select co.hadm_id
    from merged_data co
    inner join creatinine_before_non sr
    on co.hadm_id = sr.hadm_id
    and co.deathtime < (sr.charttime + interval '2' day)
);

drop materialized view if exists creatinine_discharge_non cascade;
create materialized view creatinine_discharge_non as (
    with stg_0 as (
        select *
        from merged_data co
        left join (select hadm_id, dischtime from admissions) ad using (hadm_id)
    )

    select co.hadm_id
    from stg_0 co
    inner join creatinine_before_non sr
    on co.hadm_id = sr.hadm_id
    and (co.deathtime is null or deathtime >= (sr.charttime + interval '2' day))
    and co.dischtime < (sr.charttime + interval '2' day)
);
