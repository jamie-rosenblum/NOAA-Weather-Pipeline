select
    strftime('%Y-%m', date) as month,
    station,
    avg(max_temp) as avg_max_temp,
    avg(min_temp) as avg_min_temp,
    sum(precipitation) as total_precipitation
from {{ ref('int_weather') }}
group by 
    month,
    station