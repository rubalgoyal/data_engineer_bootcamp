-- CREATE TABLE array_metrics(
-- 	user_id NUMERIC,
-- 	month_start DATE,
-- 	metric_name TEXT,
-- 	metric_array REAL[],
-- 	PRIMARY KEY(user_id, month_start, metric_name)
-- )



INSERT INTO array_metrics
with daily_aggregate AS (
	SELECT 
		user_id,
		CAST(CAST(event_time AS TIMESTAMP) AS DATE) as date_current,
		COUNT(1) AS num_hits
	FROM events
 	WHERE CAST(CAST(event_time AS TIMESTAMP) AS DATE)  = DATE('2023-01-08') 
	 		AND user_id IS NOT NULL
	GROUP BY user_id, CAST(CAST(event_time AS TIMESTAMP) AS DATE)		 	 
),
	yesterday as(
		SELECT *
		FROM array_metrics
		WHERE month_start = DATE('2023-01-01')
	)

	SELECT 
		COALESCE(da.user_id, y.user_id) AS user_id,
		CAST(COALESCE(DATE_TRUNC('month', da.date_current), y.month_start) AS DATE) as month_start,
		'site_hits' AS metric_name,
		CASE
			WHEN y.month_start IS NULL THEN ARRAY[COALESCE(da.num_hits,0)]
			WHEN y.metric_array IS NULL THEN  ARRAY_FILL(0,ARRAY[COALESCE (date_current - CAST(DATE_TRUNC('month', da.date_current) AS DATE),0)])  ||
				ARRAY[COALESCE(da.num_hits,0)]
			WHEN da.num_hits IS NULL THEN y.metric_array  ||0  
			ELSE y.metric_array  || da.num_hits
		END AS metric_array	
	FROM daily_aggregate da
	FULL OUTER JOIN yesterday y
		ON da.user_id = y.user_id
	ON CONFLICT(user_id, month_start,metric_name)
	DO
		UPDATE SET metric_array = EXCLUDED.metric_array;


SELECT * FROM array_metrics	
