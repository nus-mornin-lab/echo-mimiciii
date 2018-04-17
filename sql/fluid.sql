with fluid_0 as (
    select  *
    from
    (select icustay_id, nday, amount as input from fluid_input) fi
    full outer join
    (select icustay_id, nday, amount as output from fluid_output) fo
    using (icustay_id, nday)
)

, fluid_1 as (
    select icustay_id, nday, input, output
    from fluid_0
    where icustay_id is not null and nday is not null
)

, fluid_2 as (
    select icustay_id, nday, sum(input) as input, sum(output) as output
    from fluid_1
    group by icustay_id, nday
)

, fluid_3 as (
    select icustay_id, nday, input, output, (input - output) as balance
    from fluid_2
)

, fluid_4 as (
    select icustay_id, 
        sum(case when nday = 1 then balance else null end) as day1,
        sum(case when nday = 2 then balance else null end) as day2,
        sum(case when nday = 3 then balance else null end) as day3
    from fluid_3
    group by icustay_id
)

, fluid_5 as (
    select icustay_id, day1, day2, day3, day1 - day2 as down2, day1 - day3 as down3
    from fluid_4
)

, fluid_6 as (
    select icustay_id, day1, day2, day3, down2, down3, echo
    from merged_data co
    left join fluid_5 using (icustay_id)
)

select * from fluid_6;
