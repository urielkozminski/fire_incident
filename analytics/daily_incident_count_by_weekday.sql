SELECT
  EXTRACT(DAYOFWEEK FROM date) AS weekday,
  COUNT(*) AS total_incidents
FROM
  `fire_inc_gold.incident_count_per_day`
GROUP BY
  weekday
ORDER BY
  weekday;