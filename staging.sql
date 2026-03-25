
DROP TABLE IF EXISTS metrics.service_metrics_hourly_2;

CREATE TABLE metrics.service_metrics_hourly_2
(
    application_id            UInt32,
    service_id                UInt64,
    service                   String,
    metric                    String,
    ts_hour                   DateTime,
    success_rate_avg          Float64,
    success_rate_p50          Float64,
    success_rate_p95          Float64,
    success_rate_p25          Float64,
    success_rate_p75          Float64,
    total_windows             UInt16,
    bad_windows               UInt16,
    breach_ratio              Float64,
    slo_target                Float64,
    day_of_week               UInt8,
    hour                      UInt8,
    total_requests            UInt64,
    response_breach_count     Nullable(UInt64),

    avg_latency               Nullable(Float64),
    p90_latency               Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (application_id, service, metric, ts_hour);


DROP TABLE IF EXISTS metrics.ai_detector_staging1;

	CREATE TABLE metrics.ai_detector_staging1
	(
	  application_id UInt32,
	  service String,
	  service_id UInt32,
	  metric String,

	  pattern_type String,   -- daily_candidate | weekly_candidate | drift_candidate | sudden_candidate

	  day_of_week UInt8,    -- 1=Mon ... 7=Sun
	  hour UInt8,

	  week_of_year UInt8,
	  month UInt8,
	  year UInt16,

	  pattern_window String,

	  delta_success Float64,
	  delta_latency_p90 Nullable(Float64),

	  bad_ratio Float64,
	  total_requests UInt32,

	  ts_hour DateTime,
	  detected_at DateTime
	)
	ENGINE = MergeTree
	PARTITION BY toYYYYMM(ts_hour)
	ORDER BY (application_id, service, metric, pattern_type, week_of_year, day_of_week, hour, ts_hour);
						
	-- baseline view is created
DROP VIEW IF EXISTS metrics.ai_baseline_view_2;

CREATE OR REPLACE VIEW metrics.ai_baseline_view_2 AS
  SELECT
    application_id,
    service,
    metric,
    CASE
      WHEN metric = 'success_rate' THEN quantile(0.5)(success_rate_p50)
      WHEN metric = 'latency' THEN quantile(0.5)(success_rate_p50)
    END AS baseline_value,
    CASE
      WHEN metric = 'success_rate' THEN 0
      WHEN metric = 'latency' THEN quantile(0.5)(p90_latency)
    END AS baseline_value_p90,
    CASE
      WHEN metric = 'success_rate' THEN quantile(0.5)(total_requests)
      WHEN metric = 'latency' THEN quantile(0.5)(total_requests)
    END AS median_hour_volumne,
    sum(bad_windows) / sum(total_windows) AS breach_ratio
  FROM metrics.ai_service_features_hourly
  WHERE ts_hour >= (SELECT MAX(ts_hour) FROM metrics.ai_service_features_hourly) - INTERVAL 14 DAY
  GROUP BY application_id, service, metric

				

DROP VIEW IF EXISTS metrics.mv_daily_candidates;

	CREATE MATERIALIZED VIEW metrics.mv_daily_candidates
	TO metrics.ai_detector_staging1
	AS
	SELECT
	  f.application_id,
	  f.service,
	  f.service_id,
	  f.metric,

	  'daily_candidate' AS pattern_type,

	  toDayOfWeek(f.ts_hour) AS day_of_week,
	  f.hour,

	  toWeek(f.ts_hour) AS week_of_year,
	  toMonth(f.ts_hour) AS month,
	  toYear(f.ts_hour) AS year,

	  concat('Daily ', toString(f.hour), '-', toString((f.hour + 1) % 24)) AS pattern_window,

	  -- SLO success delta
	  f.success_rate_p50 - coalesce(b.baseline_value, 0) AS delta_success,

	  -- Latency quality delta
	  coalesce(f.p90_latency, 0) - coalesce(b.baseline_value_p90, 0) AS delta_latency_p90,

	  f.breach_ratio AS bad_ratio,
	  f.total_requests,

	  f.ts_hour,
	  now() AS detected_at
	FROM metrics.service_metrics_hourly_2 f
	JOIN metrics.ai_baseline_view_2 b
	USING (application_id, service, metric)
	WHERE
	  f.breach_ratio >= 0.4;
						
						
			-------------- weeekly--------------------------------------------------
						
DROP VIEW IF EXISTS metrics.mv_weekly_candidates;

	CREATE MATERIALIZED VIEW metrics.mv_weekly_candidates
	TO metrics.ai_detector_staging1
	AS
	SELECT
	  f.application_id,
	  f.service,
	  f.service_id,
	  f.metric,

	  'weekly_candidate' AS pattern_type,

	  toDayOfWeek(f.ts_hour) AS day_of_week,
	  f.hour,

	  toWeek(f.ts_hour) AS week_of_year,
	  toMonth(f.ts_hour) AS month,
	  toYear(f.ts_hour) AS year,

	  concat(
	    toString(toDayOfWeek(f.ts_hour)), ' ',
	    toString(f.hour), '-', toString((f.hour + 1) % 24)
	  ) AS pattern_window,

	  f.success_rate_p50 - coalesce(b.baseline_value, 0) AS delta_success,
	  CASE
	    WHEN f.metric = 'latency' THEN f.p90_latency - b.baseline_value_p90
	    ELSE NULL
	  END AS delta_latency_p90,

	  f.breach_ratio AS bad_ratio,
	  f.total_requests,

	  f.ts_hour,
	  now() AS detected_at
	FROM metrics.service_metrics_hourly_2 f
	JOIN metrics.ai_baseline_view_2 b
	USING (application_id, service, metric)
	WHERE
	  f.breach_ratio >= 0.4;
						





--instert-----
INSERT INTO metrics.service_metrics_hourly_2 (
    application_id,
    service_id,
    service,
    metric,
    ts_hour,
    success_rate_avg,
    success_rate_p50,
    success_rate_p95,
    total_windows,
    bad_windows,
    breach_ratio,
    slo_target,
    day_of_week,
    hour,
    total_requests,
    response_breach_count,
    avg_latency,
    p90_latency,
    success_rate_p25,
    success_rate_p75
)
SELECT
    application_id,
    service_id,
    service,
    metric,
    ts_hour,
    success_rate_avg,
    success_rate_p50,
    success_rate_p95,
    total_windows,
    bad_windows,
    breach_ratio,
    slo_target,
    day_of_week,
    hour,
    total_requests,
    response_breach_count,
    avg_latency,
    p90_latency,
    success_rate_p25,
    success_rate_p75
FROM metrics.ai_service_features_hourly;


CREATE OR REPLACE VIEW metrics.ai_baseline_stats_30d AS
WITH core AS (
    SELECT *
    FROM metrics.ai_baseline_view_2
),

delta_calc AS (
    SELECT
        h.application_id,
        h.service_id,
        h.service,
        h.metric,

        abs(h.success_rate_p50 - c.baseline_value) AS delta_success,
        abs(h.p90_latency - c.baseline_value_p90) AS delta_latency,

        h.ts_hour

    FROM metrics.ai_service_features_hourly h
    INNER JOIN core c
        ON h.application_id = c.application_id
       AND h.service_id     = c.service_id
       AND h.metric         = c.metric

    WHERE h.ts_hour >= (
        SELECT max(ts_hour) - INTERVAL 30 DAY
        FROM metrics.ai_service_features_hourly
    )
)

SELECT
    application_id,
    service_id,
    service,
    metric,
    quantile(0.5)(delta_success) AS delta_median_success,
    quantile(0.5)(delta_latency) AS delta_median_latency,
    count() AS observed_hours_30d,
    count() / (30 * 24.0) AS coverage_ratio_30d

FROM delta_calc
GROUP BY
    application_id,
    service_id,
    service,
    metric;