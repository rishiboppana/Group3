select * 
from {{ref('risk')}}
where risk_level ='HIGH' and close_approach_date = CURRENT_DATE()