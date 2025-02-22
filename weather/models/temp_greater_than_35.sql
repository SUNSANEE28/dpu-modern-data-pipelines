select 
    dt,
    temp,
    feels_like

from {{ source('dpu','weathers')}} 
where temp > 31