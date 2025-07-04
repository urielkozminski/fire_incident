SELECT
  CASE
    WHEN avg_response_seconds < 60 THEN '< 1 min'
    WHEN avg_response_seconds < 300 THEN '1–5 min'
    WHEN avg_response_seconds < 600 THEN '5–10 min'
    ELSE '> 10 min'
  END AS response_time_bucket,
  COUNT(*) AS days
FROM
  `project_id.fire_inc_gold.avg_response_time_by_day`
GROUP BY
  response_time_bucket
ORDER BY
  days DESC;