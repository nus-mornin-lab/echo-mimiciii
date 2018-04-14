drop materialized view if exists lactate_death cascade;
create materialized view lactate_death as (
    select hadm_id from lactate_death_echo
    union
    select hadm_id from lactate_death_non
);

drop materialized view if exists lactate_diff cascade;
create materialized view lactate_diff as (
    select hadm_id, lactate_diff from lactate_diff_echo
    union
    select hadm_id, lactate_diff from lactate_diff_non
);

drop materialized view if exists lactate_discharge cascade;
create materialized view lactate_discharge as (
    select hadm_id from lactate_discharge_echo
    union
    select hadm_id from lactate_discharge_non
);
