SELECT *
FROM (
  SELECT
    year,
    month,
    neighborhood_district,
    incident_count,
    RANK() OVER (PARTITION BY year, month ORDER BY incident_count DESC) AS rank
  FROM
    `fire_inc_gold.monthly_incidents_by_neighborhood`
)
WHERE rank <= 5
ORDER BY
  year, month, rank;