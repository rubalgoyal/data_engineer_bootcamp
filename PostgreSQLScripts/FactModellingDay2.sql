-- SELECT MIN(event_time) 
-- FROM events
-- -- This table contains every netwrok request that goes on 


-- CREATE TABLE users_cummulated (
-- 	user_id TEXT,
-- 	dates_active DATE[],
-- 	date_current DATE,
-- 	PRIMARY KEY(user_id, date_current)
-- )


-- INSERT INTO users_cummulated
-- with yesterday AS(
-- 	SELECT * 
-- 	FROM users_cummulated
-- 	WHERE date_current = DATE('2023-01-30')
-- ),
-- 	today AS(
-- 		SELECT 
-- 			CAST(user_id AS TEXT) as user_id,
-- 			DATE(CAST(event_time AS TIMESTAMP)) as active_date
-- 		FROM events
-- 		WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31') AND user_id IS NOT NULL
-- 		GROUP BY user_id,DATE(CAST(event_time AS TIMESTAMP))		
		
-- 	)

-- 	SELECT 
-- 		COALESCE(t.user_id, y.user_id) as user_id,
-- 		CASE
-- 			WHEN y.dates_active IS NULL THEN ARRAY[t.active_date]
-- 			WHEN t.active_date IS NULL THEN y.dates_active
-- 			ELSE y.dates_active || t.active_date
-- 		END as dates_active,
-- 		COALESCE(t.active_date, y.date_current + INTERVAL '1 day'  )
-- 	FROM today t
-- 	FULL OUTER JOIN yesterday y
-- 		ON t.user_id = y.user_id

--
-- with users AS (
-- 	SELECT *
-- 	FROM users_cummulated
-- 	WHERE date_current = DATE('2023-01-31') AND user_id = '406876712821807740'
-- ),
-- 	series AS(
-- 		SELECT * 
-- 		FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day' ) as series_date
-- 	) 

-- 	SELECT dates_active @> ARRAY[DATE(series_date)], * 
-- 	FROM users 
-- 	CROSS JOIN series


with users AS (
	SELECT *
	FROM users_cummulated
	WHERE date_current = DATE('2023-01-31') 
),
	series AS(
		SELECT * 
		FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day' ) as series_date
	),
	
	placeholder_ints AS(
		SELECT user_id,
			CASE 
				WHEN dates_active @> ARRAY[DATE(series_date)]
				THEN POW(2, 32 - (date_current - DATE(series_date)))
				ELSE 0
			END as placeholder_int_value	
		FROM users
		CROSS JOIN series
)	
	SELECT
		user_id,
		CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
		BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 dim_as_monthly_active

		CAST('1111111')
	FROM placeholder_ints
	GROUP BY user_id
		