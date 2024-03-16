select
EventName,
CASE WHEN Location like '%Montr%' THEN 'Montreal'
WHEN Location like '%Paulo%' THEN 'SaoPaulo'
WHEN Location like '%Portim%' THEN 'Portimao'
WHEN Location like '%rburgring%' THEN 'Nurburgring'
WHEN Location = 'Baku' THEN 'Azerbaijan'
WHEN Location = 'Marina Bay' THEN 'Singapore'
WHEN Location = 'Yas Marina' THEN 'YasIsland'
ELSE replace(Location, ' ', '') END as Location
from
ml_project_dev.f1_calender
where year(EventDate) = {partitioned_date_year}
GROUP BY 1, 2;