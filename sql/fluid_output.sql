with days as (
    (select icustay_id, intime as daystart,
        intime + interval '1' day as dayend,
        1 as nday
    from merged_data
    where deathtime is null or deathtime >= intime + interval '3' day)
    union
    (select icustay_id, intime + interval '1' day as daystart,
        intime + interval '2' day as dayend,
        2 as nday
    from merged_data
    where deathtime is null or deathtime >= intime + interval '3' day)
    union
    (select icustay_id, intime + interval '2' day as daystart,
        intime + interval '3' day as dayend,
        3 as nday
    from merged_data
    where deathtime is null or deathtime >= intime + interval '3' day)
)

, output_0 as (
    select icustay_id, nday, value as amount, charttime, daystart, dayend
    from days co
    inner join urineoutput ur using (icustay_id)
)

, output_1 as (
    select icustay_id, nday, amount, charttime, daystart, dayend
    from output_0
    where charttime >= daystart and charttime < dayend
)

, output_2 as (
    select icustay_id, nday, sum(amount) as amount
    from output_1
    group by icustay_id, nday
)

select * from output_2;
