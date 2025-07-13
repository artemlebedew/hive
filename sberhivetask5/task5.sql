use lebedevar;

set mapred.reduce.tasks = -1;
set hive.auto.convert.join = False;
set hive.exec.parallel = False;


------Создание таблицы для аналитики------
drop table if exists logs_analysis;
create external table logs_analysis (
    sample_percent float,
    overall_males_part double
)
stored as textfile;


------Выборка 0.1 %------
insert into table logs_analysis
select
    0.1 as sample_percent,
    round(sum(t5.male_visits) / (sum(t5.male_visits) + sum(t5.female_visits)), 4) as overall_males_part
from (
    select
        t4.region,
        count(case when t4.gender = 'male' then 1 end) as male_visits,
        count(case when t4.gender = 'female' then 1 end) as female_visits
    from (
        select 
            t1.region,
            t2.gender
        from ipregions t1
        inner join users t2
        on t1.ip = t2.ip
        inner join (
            select  
                tmp.ip
            from logs tablesample(5 rows) tmp
        ) t3
        on t2.ip = t3.ip
    ) t4
    group by t4.region
) t5;


------Выборка 1 %------
insert into table logs_analysis
select
    1 as sample_percent,
    round(sum(t5.male_visits) / (sum(t5.male_visits) + sum(t5.female_visits)), 4) as overall_males_part
from (
    select
        t4.region,
        count(case when t4.gender = 'male' then 1 end) as male_visits,
        count(case when t4.gender = 'female' then 1 end) as female_visits
    from (
        select 
            t1.region,
            t2.gender
        from ipregions t1
        inner join users t2
        on t1.ip = t2.ip
        inner join (
            select  
                tmp.ip
            from logs tablesample(50 rows) tmp
        ) t3
        on t2.ip = t3.ip
    ) t4
    group by t4.region
) t5;


------Выборка 5 %------
insert into table logs_analysis
select
    5 as sample_percent,
    round(sum(t5.male_visits) / (sum(t5.male_visits) + sum(t5.female_visits)), 4) as overall_males_part
from (
    select
        t4.region,
        count(case when t4.gender = 'male' then 1 end) as male_visits,
        count(case when t4.gender = 'female' then 1 end) as female_visits
    from (
        select 
            t1.region,
            t2.gender
        from ipregions t1
        inner join users t2
        on t1.ip = t2.ip
        inner join (
            select  
                tmp.ip
            from logs tablesample(250 rows) tmp
        ) t3
        on t2.ip = t3.ip
    ) t4
    group by t4.region
) t5;


------Выборка 10 %------
insert into table logs_analysis
select
    10 as sample_percent,
    round(sum(t5.male_visits) / (sum(t5.male_visits) + sum(t5.female_visits)), 4) as overall_males_part
from (
    select
        t4.region,
        count(case when t4.gender = 'male' then 1 end) as male_visits,
        count(case when t4.gender = 'female' then 1 end) as female_visits
    from (
        select 
            t1.region,
            t2.gender
        from ipregions t1
        inner join users t2
        on t1.ip = t2.ip
        inner join (
            select  
                tmp.ip
            from logs tablesample(500 rows) tmp
        ) t3
        on t2.ip = t3.ip
    ) t4
    group by t4.region
) t5;


set mapred.reduce.tasks = 80;
set hive.auto.convert.join = False;
set hive.exec.parallel = True;


------Выборка 50 %------
insert into table logs_analysis
select
    50 as sample_percent,
    round(sum(t5.male_visits) / (sum(t5.male_visits) + sum(t5.female_visits)), 4) as overall_males_part
from (
    select
        t4.region,
        count(case when t4.gender = 'male' then 1 end) as male_visits,
        count(case when t4.gender = 'female' then 1 end) as female_visits
    from (
        select 
            t1.region,
            t2.gender
        from ipregions t1
        inner join users t2
        on t1.ip = t2.ip
        inner join (
            select  
                tmp.ip
            from logs tablesample(2500 rows) tmp
        ) t3
        on t2.ip = t3.ip
    ) t4
    group by t4.region
) t5;


------Истинное значение (выборка 100 %)------
insert into table logs_analysis
select
    100 as percentage,
    round(sum(t5.male_visits) / (sum(t5.male_visits) + sum(t5.female_visits)), 4) as overall_males_part
from (
    select
        t4.region,
        count(case when t4.gender = 'male' then 1 end) as male_visits,
        count(case when t4.gender = 'female' then 1 end) as female_visits
    from (
        select 
            t1.region,
            t2.gender
        from ipregions t1
        inner join users t2
        on t1.ip = t2.ip
        inner join logs t3
        on t2.ip = t3.ip
    ) t4
    group by t4.region
) t5;