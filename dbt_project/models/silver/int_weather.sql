select 
    date,
    station,
    max(case when datatype = 'TMAX' then value end) as max_temp,
    max(case when datatype = 'TMIN' then value end) as min_temp,
    max(case when datatype = 'PRCP' then value end) as precipitation
from {{ ref('stg_weather') }}
group by 
    date,
    station