DROP TABLE IF EXISTS F1_CALENDER;

create table F1_CALENDER (
RoundNumber INT,
EventDate DATETIME,
EventName  VARCHAR(30),
Location VARCHAR(30),
EventFormat VARCHAR(30),
Session1  VARCHAR(30),
Session1DateUtc DATETIME,
Session2  VARCHAR(30),
Session2DateUtc DATETIME,
Session3  VARCHAR(30),
Session3DateUtc DATETIME,
Session4  VARCHAR(30),
Session4DateUtc DATETIME,
Session5  VARCHAR(30),
Session5DateUtc DATETIME
)