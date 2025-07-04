SELECT
  city,
  fire_fatalities_total,
  incidents_with_fatalities
FROM
  `fire_inc_gold.fact_fire_fatalities_by_city`
ORDER BY
  fire_fatalities_total DESC
LIMIT 10;