with serum_1 as (
    select hadm_id, valuenum, charttime,
        case when itemid in (50813) then 'lactate'
             when itemid in (50912) then 'creatinine'
        else null end as label
    from labevents
    where itemid in (50813, 50912)
    and valuenum is not null
    and valuenum > 0
)

, serum_before as (
    select distinct co.hadm_id, sr.label,
        first_value(valuenum) over (partition by co.hadm_id, sr.label order by sr.charttime desc) as serum_before
    from merged_data co
    left join serum_1 sr on co.hadm_id = sr.hadm_id
        and sr.charttime >= co.intime and date_trunc('day', sr.charttime) <= co.echo_time
        -- and sr.charttime between co.echo_time and co.intime
        -- and sr.charttime >= date_trunc('day', co.intime) and sr.charttime < (co.echo_time + interval '2' day)
)

, serum_after as (
    select distinct co.hadm_id, sr.label,
        first_value(valuenum) over (partition by co.hadm_id, sr.label order by sr.charttime) as serum_after
    from merged_data co
    left join serum_1 sr on co.hadm_id = sr.hadm_id
        and sr.charttime between (co.echo_time + interval '2' day) and co.outtime
)

, serum_echo_diff as (
    select hadm_id, label, serum_before - serum_after as serum_diff
    from serum_before
    inner join serum_after using (hadm_id, label)
)

-- , unpivot as (
--     select hadm_id,
--         max(case when label = 'creatinine' then serum_diff else null end) as creatinine_diff,
--         max(case when label = 'lactate' then serum_diff else null end) as lactate_diff
--     from serum_echo_diff
--     group by hadm_id
-- )

-- , serum_echo_diff_final as (
--     select * from unpivot
-- )

select * from serum_echo_diff;







