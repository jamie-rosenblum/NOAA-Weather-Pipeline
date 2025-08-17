select  
    date,
    station,
    datatype,
    value
from {{ source('raw', 'weather') }}