select temp from {{ref('temp_greater_than_35')}}
where temp < 35