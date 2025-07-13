use lebedevar;

set mapred.reduce.tasks = 80;
set hive.auto.convert.join = False;
set hive.exec.parallel=True;

select
    t4.region as region,
    sum(if(t4.gender = 'male', 1, 0)) as male_visits,
    sum(if(t4.gender = 'female', 1, 0)) as female_visits
from (
    select
        t1.region as region,
        t2.gender as gender
    from ipregions t1
    inner join users t2
    on t1.ip = t2.ip
    inner join logs t3
    on t2.ip = t3.ip
) t4
group by t4.region
limit 10;