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
track_data a
join dim_event b
on a.event_cd = b.event_cd;