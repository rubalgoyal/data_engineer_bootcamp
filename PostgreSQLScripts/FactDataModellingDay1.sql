-- SELECT * FROM game_details
-- Game_details table is not very efficent and most of the things are wrong. 

SELECT 
	game_id, team_id, player_id, COUNT(game_id)--     1. For game_details we going to work with  game_id, team_id, player_id.  Once we identify the grain  of the table which is likely for every game, every team and every player every tam we have the unique identifier.
FROM game_details
GROUP BY game_id, team_id, player_id
HAVING COUNT(game_id) > 1;   --1. First we will identify the duplicates
-- after this we will have 2 of every records in here.
-- Now, first thing we want to do the filter here to get rid of duplicates

--1. Now we will create deduped and then we will move the above query in this and to remove duplicates modify this to get same columns
With deduped AS (
	SELECT 
		*, ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ) AS row_num
	FROM game_details	
)

SELECT * FROM deduped
ORDER BY row_num DESC
-- After running this we know we have duplicate records. 

-- To get rid f duplicate records we will change this where row_num = 1
With deduped AS (
	SELECT 
		*, ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ) AS row_num
	FROM game_details	
)


SELECT * FROM deduped
WHERE row_num = 1
-- This will be the start query we are going to work with 
-- Alot of time we do ORDER BY in parttion so that we can always pick the First row. But the intersting thing of this data set 
--  there is nothing to ORDER BY. Thats the one of the problem with this dataset which we will solve.
-- We dont need some columns from table which dont matter for us. This Fact is very denormalized.So in this dataset we have both things 
--  columns we dont need and colums that are missing. One of the thing from Fact presentation there is no when column.

-- When column we will get that from game
 With deduped AS (
	SELECT g.game_date_est,
		gd.*,	
		ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est ) AS row_num
	FROM game_details gd
	JOIN games g
		ON gd.game_id = g.game_id
)
SELECT * FROM deduped
WHERE row_num = 1

-- after this game_details is really denormalized and to correct that we choose some columns which are neccesaary to store in game_details 
-- we wrote this query to make table more efficent

With deduped AS (
	SELECT g.game_date_est,
		g.season,
		g.home_team_id,
		gd.*,	
		ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est ) AS row_num
	FROM game_details gd
	JOIN games g
		ON gd.game_id = g.game_id
)

-- Now we hve efficent data set and we can all the details such player info or team info by joining player and 
-- team tables with this table ehich is very cheap
With deduped AS (
	SELECT g.game_date_est,
		g.season,
		g.home_team_id,
		gd.*,	
		ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est ) AS row_num
	FROM game_details gd
	JOIN games g
		ON gd.game_id = g.game_id
)
SELECT 
	game_date_est,
	season,
	team_id,
	team_id = home_team_id AS dim_is_playing_at_home,
	player_id,
	player_name,
	start_position,
	COALESCE(POSITION('DNP' in comment),0) > 0 as dim_did_not_play,
	COALESCE(POSITION('DND' in comment),0) > 0 as dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment),0) > 0 as dim_not_with_play,
	CAST(SPLIT_PART(min, ':',1)AS REAL) + CAST(SPLIT_PART(min, ':',2)AS REAL)/60 AS minutes,
	fgm,
	fga,
	fg3m,
	fg3a,
	ftm,
	fta
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	"TO" as turnovers,
	pf,
	pts
	plus_minus
	
FROM deduped
WHERE row_num = 1

-- dim columns are you will filter out and m are you will aggregrate and like to o all stuff on 
CREATE TABLE fact_game_details(
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_start_position TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
)

-- Insert data as per table
INSERT INTO fact_game_details
With deduped AS (
	SELECT g.game_date_est,
		g.season,
		g.home_team_id,
		gd.*,	
		ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est ) AS row_num
	FROM game_details gd
	JOIN games g
		ON gd.game_id = g.game_id
)
SELECT 
	game_date_est AS dim_game_date,
	season AS dim_Season,
	team_id AS dim_team_id,
	player_id AS dim_player_id,
	player_name AS dim_player_name,
	start_position AS dim_start_posiiton,
	team_id = home_team_id AS dim_is_playing_at_home,	
	COALESCE(POSITION('DNP' in comment),0) > 0 as dim_did_not_play,
	COALESCE(POSITION('DND' in comment),0) > 0 as dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment),0) > 0 as dim_not_with_play,
	CAST(SPLIT_PART(min, ':',1)AS REAL) + CAST(SPLIT_PART(min, ':',2)AS REAL)/60 AS m_minutes,
	fgm AS m_fgm, 
	fga AS m_fga,
	fg3m AS m_fg3m,
	fg3a AS m_fg3a,
	ftm AS m_ftm,
	fta AS m_fta,
	oreb AS m_oreb,
	dreb AS m_dreb,
	reb AS m_reb,
	ast AS m_ast,
	stl AS m_stl,
	blk AS m_blk,
	"TO" as m_turnovers,
	pf AS m_pf,
	pts AS m_pts,
	plus_minus AS m_plus_minus
	
FROM deduped
WHERE row_num = 1

SELECT * FROM fact_game_details


-- to get infor about teams as we lost in this table so this not an expensive query we can easlily get details of team
SELECT t.*, gd.* FROM fact_game_details gd JOIN teams t
ON t.team_id = gd.dim_team_id

-- Lets find player who was not with team
SELECT
	dim_player_name, 
	COUNT(1) AS num_games,
	COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS baliled_num,
	CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL)/COUNT(1) AS bailed_pct
FROM fact_game_details
GROUP BY dim_player_name
ORDER BY 4 DESC
	

-- Lets find player who was not with team when they were at home

SELECT
	dim_player_name, 
	dim_is_playing_at_home,
	COUNT(1) AS num_games,
	SUM(m_pts) AS total_points,
	COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS baliled_num,
	CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL)/COUNT(1) AS bailed_pct
FROM fact_game_details
GROUP BY dim_player_name,dim_is_playing_at_home
ORDER BY 6 DESC