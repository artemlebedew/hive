use lebedevar;

add file ./script.py;

select transform(ip, request_date, request, page_size, status, app_client)
using 'python3 ./script.py' as (
	ip,
	request_date,
	request,
	page_size,
	status,
	app_client)
from Logs
limit 10;