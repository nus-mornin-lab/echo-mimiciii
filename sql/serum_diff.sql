with serum_0 as (
    select *
    from
    (select hadm_id, label, serum_diff as echo_diff from serum_echo_diff) es
    full outer join
    (select hadm_id, label, serum_diff as non_diff from serum_non_diff) ns
    using (hadm_id, label)
)

, serum_1 as (
    select * from serum_0 where hadm_id is not null and label is not null
)

, serum_2 as (
    select hadm_id, label, echo, echo_diff, non_diff
    from serum_1
    left join merged_data co using (hadm_id)
)

, serum_3 as (
    select hadm_id, label, echo,
        case when echo = 1 then echo_diff else non_diff end as diff
    from serum_2
)

, unpivot as (
    select hadm_id,
        max(case when label = 'creatinine' then diff else null end) as creatinine_diff,
        max(case when label = 'lactate' then diff else null end) as lactate_diff
    from serum_3
    group by hadm_id
)

select * from unpivot;
