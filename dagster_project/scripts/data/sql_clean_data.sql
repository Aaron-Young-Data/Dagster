SELECT
LapTimeFP1,
Sector1TimeFP1,
Sector2TimeFP1,
Sector3TimeFP1,
CompoundFP1,
AirTempFP1,
RainfallFP1,
TrackTempFP1,
WindDirectionFP1,
WindSpeedFP1,
IFNULL(LapTimeFP2, 0) AS LapTimeFP2,
IFNULL(Sector1TimeFP2, 0) AS Sector1TimeFP2,
IFNULL(Sector2TimeFP2, 0) AS Sector2TimeFP2,
IFNULL(Sector3TimeFP2, 0) AS Sector3TimeFP2,
IFNULL(CompoundFP2, 0) AS CompoundFP2,
IFNULL(AirTempFP2, 0) AS AirTempFP2,
IFNULL(RainfallFP2, 0) AS RainfallFP2,
IFNULL(TrackTempFP2, 0) AS TrackTempFP2,
IFNULL(WindDirectionFP2, 0) AS WindDirectionFP2,
IFNULL(WindSpeedFP2, 0) AS WindSpeedFP2,
IFNULL(LapTimeFP3, 0) AS LapTimeFP3,
IFNULL(Sector1TimeFP3, 0) AS Sector1TimeFP3,
IFNULL(Sector2TimeFP3, 0) AS Sector2TimeFP3,
IFNULL(Sector3TimeFP3, 0) AS Sector3TimeFP3,
IFNULL(CompoundFP3, 0) AS CompoundFP3,
IFNULL(AirTempFP3, 0) AS AirTempFP3,
IFNULL(RainfallFP3, 0) AS RainfallFP3,
IFNULL(TrackTempFP3, 0) AS TrackTempFP3,
IFNULL(WindDirectionFP3, 0) AS WindDirectionFP3,
IFNULL(WindSpeedFP3, 0) AS WindSpeedFP3,
LapTimeQ,
AirTempQ,
RainfallQ,
WindDirectionQ,
WindSpeedQ,
sprint_flag as is_sprint,
traction,
tyre_stress,
asphalt_grip,
braking,
asphalt_abrasion,
lateral_force,
track_evolution,
downforce FROM cleaned_session_data
where sprint_flag = 1 and
LapTimeQ is not NULL
UNION
SELECT
LapTimeFP1,
Sector1TimeFP1,
Sector2TimeFP1,
Sector3TimeFP1,
CompoundFP1,
AirTempFP1,
RainfallFP1,
TrackTempFP1,
WindDirectionFP1,
WindSpeedFP1,
LapTimeFP2,
Sector1TimeFP2,
Sector2TimeFP2,
Sector3TimeFP2,
CompoundFP2,
AirTempFP2,
RainfallFP2,
TrackTempFP2,
WindDirectionFP2,
WindSpeedFP2,
LapTimeFP3,
Sector1TimeFP3,
Sector2TimeFP3,
Sector3TimeFP3,
CompoundFP3,
AirTempFP3,
RainfallFP3,
TrackTempFP3,
WindDirectionFP3,
WindSpeedFP3,
LapTimeQ,
AirTempQ,
RainfallQ,
WindDirectionQ,
WindSpeedQ,
sprint_flag as is_sprint,
traction,
tyre_stress,
asphalt_grip,
braking,
asphalt_abrasion,
lateral_force,
track_evolution,
downforce FROM cleaned_session_data
where sprint_flag = 0 and
LapTimeQ is not NULL