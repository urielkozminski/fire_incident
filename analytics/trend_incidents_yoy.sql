SELECT
  EXTRACT(YEAR FROM date) AS year,
  COUNT(*) AS total_incidents
FROM
  `fire_inc_gold.incident_count_per_day`
GROUP BY
  year
ORDER BY
  year;