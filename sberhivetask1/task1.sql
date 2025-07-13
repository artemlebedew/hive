use lebedevar;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=116;
set hive.exec.max.dynamic.partitions.pernode=116;
add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

drop table if exists Logs;
drop table if exists Logs_stage;
drop table if exists Users;
drop table if exists IPRegions;
drop table if exists Subnets;


------Создание таблицы Logs------
create external table Logs_stage (
    ip string,
    request_date int,
    request string,
    page_size smallint,
    status smallint,
    app_client string
)
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
    "input.regex" = '^((?:\\d{1,3}\\.){3}\\d{1,3})\\s+(\\d{8})\\d+\\s+([fhtpw]{3,4}:\\S+)\\s+(\\d+)\\s+(\\d+)\\s+(.*)$'
)
stored as textfile
location '/data/user_logs/user_logs_M';

create external table Logs (
    ip string,
    request string,
    page_size smallint,
    status smallint,
    app_client string
)
partitioned by (request_date int)
stored as textfile;

insert overwrite table Logs partition (request_date)
select 
    ip,
    request,
    page_size,
    status,
    app_client,
    request_date
from Logs_stage;
drop table if exists Logs_stage;


------Создание таблицы Users------
create external table Users (
        ip string,
        browser string,
        gender string,
        age tinyint
)
row format delimited fields terminated by '\t'
stored as textfile
location '/data/user_logs/user_data_M';


------Создание таблицы IPRegions------
create external table IPRegions (
        ip string,
        region string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/data/user_logs/ip_data_M';


------Создание таблицы Subnets------
create external table Subnets (
        ip string,
        mask string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/data/subnets/variant1';


------Вывод результатов------
select * from Logs limit 10;
select * from Users limit 10;
select * from IPRegions limit 10;
select * from Subnets limit 10;