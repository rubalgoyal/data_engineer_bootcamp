INSERT INTO vertices
-- created first vertices as per this
SELECT
	game_id AS identifier,
	'game'::vertex_type AS type,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
	) AS properties

FROM games;

INSERT INTO vertices 
WITH players_agg AS (
-- player detalis
	SELECT 
		player_id AS identifier,
		MAX(player_name) AS player_name,
		COUNT(1) AS number_of_games,
		SUM(pts) AS total_points,
		ARRAY_AGG(DISTINCT team_id) AS teams
	FROM game_details
	GROUP BY player_id
)

SELECT identifier, 'player'::vertex_type,
		json_build_object(
				'player_name', player_name,
				'number_of_games', number_of_games,
				'total_points', total_points,
				'teams', teams						
		)
FROM players_agg;


INSERT INTO vertices
WITH teams_deduped AS(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
	FROM teams
)
SELECT 
	team_id AS identifier,
	'team'::vertex_type AS type,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_found', yearfounded
	)
FROM teams_deduped
WHERE row_num = 1;

-- we have data as of now in our table, just checking the numbers
SELECT type, COUNT(1)
FROM vertices
GROUP BY 1;


-- looking at the edges now, play_in is the easist way to start in
INSERT INTO edges
WITH deduped AS(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY player_id, game_id) as row_num
	FROM game_details
)
SELECT 
	player_id AS subject_identifier,
	'player':: vertex_type as subject_type,
	game_id AS object_identifier,
	'game'::vertex_type AS object_type,
	'plays_in'::edge_type AS edge_type,
	json_build_object(
		'start_position', start_position,
		'pts',pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	)as properties
FROM deduped
WHERE row_num = 1;

SELECT 
	v.properties->>'player_name',
	MAX(e.properties->>'pts')
FROM vertices v 
JOIN edges e
	ON e.subject_identifier = v.identifier
	AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC

SELECT 
	v.properties->>'player_name',
	MAX(CAST(e.properties->>'pts' AS INTEGER))
FROM vertices v 
JOIN edges e
	ON e.subject_identifier = v.identifier
	AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

INSERT INTO edges
-- want to create 
WITH deduped AS(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY player_id, game_id) as row_num
	FROM game_details
),
	filtered AS (
		SELECT * FROM deduped
		WHERE row_num = 1
	),
	aggregated AS(
-- cretae an edge that plays again between players
			SELECT
				f1.player_id as subject_player_id,
				-- f1.player_name as subject_player_name,
				f2.player_id as object_player_id,
				-- f2.player_name as object_player_name,
				CASE WHEN f1.team_abbreviation = f2.team_abbreviation
					 THEN 'shares_team'::edge_type
			    ELSE 'play_against'::edge_type
				END as edge_type,
				MAX(f1.player_name) as subject_player_name,
				MAX(f2.player_name) as object_player_name,
				COUNT(1) AS num_games,
				SUM(f1.pts) AS subject_points,
				SUM(f2.pts) AS object_points
			FROM filtered f1
			JOIN filtered f2
				ON f1.game_id = f2.game_id
				AND f1.player_name <> f2.player_name
			WHERE f1.player_id > f2.player_id	
			GROUP BY f1.player_id, f2.player_id,
					CASE WHEN f1.team_abbreviation = f2.team_abbreviation
					 	THEN 'shares_team'::edge_type
			    	ELSE 'play_against'::edge_type
					END
	)
	SELECT  subject_player_id as subject_identifier,
			'player'::vertex_type AS subject_type,
			object_player_id AS object_identifier,
			'player'::vertex_type AS object_type,
			edge_type AS edge_type,
			json_build_object(
				'num_games', num_games,
				'subject_points', subject_points,
				'object_points', object_points
			)
	FROM aggregated


	SELECT v.properties->>'player_name',
		 e.object_identifier,
		CAST(v.properties->>'number_of_games' as REAL)/
		CASE WHEN CAST(v.properties->>'total_points' as REAL) = 0 THEN 1
		ELSE CAST(v.properties->>'total_points' as REAL)
		END,
		e.properties->>'subject_points',
		e.properties->>'num_games'
	FROM vertices v JOIN  edges e
		ON v.identifier = e.subject_identifier
		AND v.type = e.subject_type
	WHERE e.object_type = 'player'::vertex_type;
	