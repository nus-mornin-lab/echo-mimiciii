drop materialized view if exists creatinine_death cascade;
create materialized view creatinine_death as (
    select hadm_id from creatinine_death_echo
    union
    select hadm_id from creatinine_death_non
);

drop materialized view if exists creatinine_diff cascade;
create materialized view creatinine_diff as (
    select hadm_id, creatinine_diff from creatinine_diff_echo
    union
    select hadm_id, creatinine_diff from creatinine_diff_non
);

drop materialized view if exists creatinine_discharge cascade;
create materialized view creatinine_discharge as (
    select hadm_id from creatinine_discharge_echo
    union
    select hadm_id from creatinine_discharge_non
);
