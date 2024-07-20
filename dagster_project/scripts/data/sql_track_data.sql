SELECT
b.event_name,
a.traction,
a.tyre_stress,
a.asphalt_grip,
a.braking,
a.asphalt_abrasion,
a.lateral_force,
a.track_evolution,
a.downforce
FROM
dim_track a
join dim_event b
on a.track_cd = b.track_cd
group by 1,2,3,4,5,6,7,8,9;