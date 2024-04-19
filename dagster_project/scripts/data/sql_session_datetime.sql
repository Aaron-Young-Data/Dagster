Select
EventFormat,
Session4DateUtc,
Session2DateUtc
FROM
f1_calender
WHERE EventName = '{event}' and
year(EventDate) = {year}