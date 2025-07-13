use lebedevar;

select
    request_date,
    count(1) as cnt
from logs
group by request_date
order by cnt desc;