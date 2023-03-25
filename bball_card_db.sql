
/* use master
go
drop database baseball_cards_db
go */

/* create database baseball_cards_db
GO  */
select * from cards

use baseball_cards_db
GO
-- Down / Reset
drop table if exists bills_cards
drop table if exists cards
drop table if exists fielders
drop table if exists pitchers
drop table if exists player_stats_by_year
drop table if exists teams
drop table if exists players_attributes
drop table if exists players

-- create tables
create table players_attributes (
    player_id int identity primary key,
    player_firstname varchar(50) not NULL,
    player_lastname varchar(50) not null,
    player_birthdate date null,
    player_deathdate date null,
    player_height int null,
    player_weight int null,
    player_bats int null,
    player_throws varchar(5)
)

create table teams (
    team_id int identity primary key,
    team_name varchar(50) not null,
    team_park varchar(50) not null,
    team_year date not null
        check (team_year >= 1970 and team_year <= 1989),
    team_league varchar(50),
    team_division_winner char(1) not NULL
        check (team_division_winner in ('Y','N')),
    team_wild_card_winner char(1) not NULL
        check (team_wild_card_winner in ('Y','N')),
    team_league_champion char(1) not NULL
        check (team_league_champion in ('Y','N')),
    team_world_series_chamption char(1) not NULL
        check (team_league_champion in ('Y','N'))
)

create table players_stats_by_year (
    stats_id int identity primary key,
    player_id int not null foreign key references players_attributes (player_id),
    team_id int not null foreign key references teams (team_id),
    stats_year year not null,
    stats_league varchar(50) not null,
    stats_games int not null,
    stats_position varchar(10) not null,
    stats_at_bat int, 
    stats_wins int,
    stats_losses int,
    stats_runs int,
    stats_hits int,
    stats_doubles int,
    stats_triples int,
    stats_home_runs int,
    stats_rbi int,
    stats_stolen_bases int,
    stats_hit_by_pitch int,
    stats_sac_hits int,
    stats_sac_flies int,
    stats_grd_dbl_play int,
    stats_time_played int,
    stats_putouts int,
    stats_assists int,
    stats_errors int,
    stats_dbl_plays int,
    stats_passed_balls_c int,
    stats_wild_pitches_c int,
    stats_opp_stolen_base_c int,
    stats_strikeouts int, 
    stats_opp_batting_avg float,
    stats_games_started int,
    stats_completed_games int,
    stats_shutouts int,
    stats_saves int,
    stats_era float,
    stats_intentional_walk int,
    stats_wild_pitches int,
    stats_batters_hit_by_pitch int,
    stats_runs_allowed int
)

create table pitchers (
    pitcher_id int identity primary key,
    player_id int not null foreign key references players_attributes (player_id)
)

create table fielders (
    fielder_id int identity primary key, 
    player_id int not null foreign key references player_attributes (player_id),
    position varchar(2) not null
)

--created table bills_cards and then used Ryan's Python routine to load the data
/*create table bills_cards (
    bills_card_id int identity primary key,
    bills_card_cert int not null UNIQUE,
    bills_card_spec int not null,
    bills_card_num int not null,
    bills_card_year int not null,
    bills_card_desc varchar(100) not null,
    bills_card_grade float not NULL,
    bills_card_pop int not null,
    bills_card_pop_higher int
)
select c.card_desc,c.card_player_id,p.player_id,p.player_url from cards c join players p on c.card_player_id=p.player_id order by p.player_id
*/

SELECT * FROM information_schema.columns WHERE table_name = 'teams'

drop table if exists cards
--TABLE CARDS...Loaded data into cards from a .csv using the Azure extension and then altered it to add an ID, PK, FK, and a composite unique constraint
Alter Table cards
    Add card_id Int Identity(1, 1)
Alter Table cards
    Add primary key (card_id)
ALTER TABLE cards
    ADD CONSTRAINT uq_card_num_year UNIQUE(card_year, card_num);
Alter TABLE cards
    ADD Foreign key (card_player_id)
    references players(player_id)

drop table if exists players
--TABLE PLAYERS...Loaded data into players from a .csv using the Azure extension and then altered it to add an ID ad a PK
Alter Table players
    Add player_id Int Identity(1, 1)
Alter Table players
    Add primary key (player_id)
CREATE UNIQUE INDEX uq_player_url
  ON players (player_url)
  WHERE player_url IS NOT NULL


drop table if exists bills_cards
--TABLE BILLS_CARDS...Loaded data into bills_cards from a .csv using the Azure extension and then altered it to add an ID, PK, FK, and a unique constraint
Alter Table bills_cards
    Add bills_card_id Int Identity(1, 1)
Alter Table bills_cards
    Add primary key (bills_card_id)
ALTER TABLE bills_cards
    ADD CONSTRAINT uq_bills_card_cert UNIQUE(bills_card_cert)
Alter TABLE bills_cards
    ADD Foreign key (bills_card_player_id)
    references players(player_id)

drop table if exists teams
-- TABLE TEAMS...Loaded data into teams from a .csv using the Azure extension.  No alterations needed.

select * from cards left join players on cards.card_player_id=players.player_id
select * from teams


----- script to merge players_id from players into stats and update stats table -----

alter table players
    add player_url_player_id varchar(20)
update players
    set player_url_player_id = SUBSTRING(player_url, CHARINDEX('p=', player_url) + 2, 9)

alter table player_stats_by_year
    add players_player_id int 

update stats
    set stats.players_player_id = ply.player_id
from player_stats_by_year as stats
    join players as ply on stats.player_id = ply.player_url_player_id

alter table player_stats_by_year
    add 
        stats_outs_pitched int,
        stats_earned_runs int,
        stats_walks int,
        stats_batters_hit_by_pitch int,
        stats_balks int,
        stats_batters_faced_by_pitcher int,
        stats_runs_allowed int



-- outs pitched
update stats
    set stats.stats_outs_pitched = pit.IPouts
from player_stats_by_year as stats
    join pitchers as pit on stats.player_id = pit.playerID
where stats.stats_year = pit.yearID

-- earned runs
update stats
    set stats.stats_earned_runs = pit.ER
from player_stats_by_year as stats
    join pitchers as pit on stats.player_id = pit.playerID
where stats.stats_year = pit.yearID

-- batters hit by pitch
update stats
    set stats.stats_batters_hit_by_pitch = pit.HBP
from player_stats_by_year as stats
    join pitchers as pit on stats.player_id = pit.playerID
where stats.stats_year = pit.yearID

-- balks
update stats
    set stats.stats_balks = pit.BK
from player_stats_by_year as stats
    join pitchers as pit on stats.player_id = pit.playerID
where stats.stats_year = pit.yearID

-- batters faced by pitcher
update stats
    set stats.stats_batters_faced_by_pitcher = pit.BFP
from player_stats_by_year as stats
    join pitchers as pit on stats.player_id = pit.playerID
where stats.stats_year = pit.yearID


-- runs allowed
update stats
    set stats.stats_runs_allowed = pit.R
from player_stats_by_year as stats
    join pitchers as pit on stats.player_id = pit.playerID
where stats.stats_year = pit.yearID

--USE CASE 5 - Upsert into Bills_cards
-- down
GO
drop procedure if exists p_upsert_bills_cards
drop type if exists bills_upsert_type

--up
-- creating temp table bills_upsert_type to field multi row inputs
GO
CREATE TYPE bills_upsert_type AS TABLE
(card_cert INT,
card_spec INT,
card_num INT,
card_year INT,
card_psa_desc nvarchar(100),
card_grade float,
card_pop int,
card_pop_higher int,
card_stat_year int,
card_player_id int
)

GO
CREATE PROCEDURE p_upsert_bills_cards
    @var_bills_upserts 
    bills_upsert_type READONLY
AS
BEGIN

SET NOCOUNT ON  
MERGE INTO bills_cards AS target
    USING (
        SELECT card_cert, card_spec, card_num, card_year, card_psa_desc, card_grade, card_pop, 
        card_pop_higher, card_stat_year, card_player_id
        FROM @var_bills_upserts
    ) AS source (card_cert, card_spec, card_num, card_year, card_psa_desc, card_grade, card_pop, 
    card_pop_higher, card_stat_year, card_player_id)
ON (target.bills_card_cert = source.card_cert)

WHEN MATCHED THEN
UPDATE SET
    bills_card_spec = source.card_spec,
    bills_card_num = source.card_num,
    bills_card_year = source.card_year,
    bills_card_psa_desc = source.card_psa_desc,
    bills_card_grade = source.card_grade,
    bills_card_pop = source.card_pop,
    bills_card_pop_higher = source.card_pop_higher,
    bills_card_stat_year = source.card_stat_year,
    bills_card_player_id = source.card_player_id

WHEN NOT MATCHED THEN
    INSERT (bills_card_cert, bills_card_spec, bills_card_num, bills_card_year, bills_card_psa_desc, bills_card_grade, 
    bills_card_pop, bills_card_pop_higher, bills_card_stat_year, bills_card_player_id)
    VALUES (source.card_cert, source.card_spec, source.card_num, source.card_year, source.card_psa_desc, source.card_grade, 
    source.card_pop, source.card_pop_higher, source.card_stat_year, source.card_player_id);
END

GO
-- declaring a table variable "var_upsert_bills_cards" with insert values that would come from the user interface
DECLARE @var_upsert_bills_cards AS bills_upsert_type
INSERT INTO @var_upsert_bills_cards (card_cert, card_spec, card_num, card_year, card_psa_desc, card_grade, 
card_pop, card_pop_higher, card_stat_year, card_player_id)
VALUES (1234567890, 1234567890, 555, 1986, 'TESTING BILLS UPSERT', 9, 23, 6, 1985, 687)
INSERT INTO @var_upsert_bills_cards (card_cert, card_spec, card_num, card_year, card_psa_desc, card_grade, 
card_pop, card_pop_higher, card_stat_year, card_player_id)
VALUES (1111111, 2222222, 666, 1986, 'TESTING BILLS UPSERT2', 8.5, 45, 10, 1985, 687)
-- executing my stored procedure (p_upsert_bills_cards) to affect the table bills_cards
EXEC p_upsert_bills_cards @var_upsert_bills_cards

GO
-- declaring a table variable "var_upsert_bills_cards" with update values that would come from the user interface
DECLARE @var_upsert_bills_cards AS bills_upsert_type
INSERT INTO @var_upsert_bills_cards (card_cert, card_spec, card_num, card_year, card_psa_desc, card_grade, 
card_pop, card_pop_higher, card_stat_year, card_player_id)
VALUES (1234567890, 1234567890, 555, 1986, 'UPDATING 1ST TEST', 9, 23, 6, 1985, 687)
INSERT INTO @var_upsert_bills_cards (card_cert, card_spec, card_num, card_year, card_psa_desc, card_grade, 
card_pop, card_pop_higher, card_stat_year, card_player_id)
VALUES (1111111, 2222222, 666, 1986, 'UPDATING 2ND TEST', 8.5, 45, 10, 1985, 687)
-- executing my stored procedure (p_upsert_bills_cards) to affect the table bills_cards
EXEC p_upsert_bills_cards @var_upsert_bills_cards

-- just checking results
GO
delete from bills_cards where bills_card_cert=1234567890 or bills_card_cert=1111111
select * from bills_cards where bills_card_cert=1234567890 or bills_card_cert=1111111

--END USE CASE 5

Select * from cards
select * from player_stats_by_year
select * from pitchers




--Andrew Alford Stored Procedure Use Case 1:
DROP PROCEDURE IF EXISTS get_player_stats
GO
CREATE PROCEDURE get_player_stats
    @year_id INT = NULL
  , @player_first_name VARCHAR(50) = NULL
  , @player_last_name VARCHAR(50) = NULL
  , @pitchers BIT = NULL
  , @fielders BIT = NULL
  , @personal BIT = NULL
  , @professional BIT = NULL
AS
BEGIN
  DECLARE @rowcount INT
  IF (@personal = 1)
  BEGIN
    SELECT p.nameFirst
      , p.nameLast
      , p.birthYear
      , p.birthCountry
      , p.birthState
      , p.deathYear
      , p.height
      , p.weight
      , p.bats
      , p.throws
      , p.debut
      , p.finalGame
    FROM people as p
    WHERE (@player_first_name IS NULL OR p.nameFirst LIKE '%' + @player_first_name + '%')
      AND (@player_last_name IS NULL OR p.nameLast LIKE '%' + @player_last_name + '%')
    SET @rowcount = @@ROWCOUNT
    IF (@rowcount > 500)
    BEGIN
      RAISERROR ('More than 500 rows returned, please refine your search!', 16, 1)
      RETURN
    END
  END
  IF (@professional = 1)
  BEGIN
    SELECT p.nameFirst
      , p.nameLast
      , s.team_id
      , s.stats_year
      , s.stats_league
      , s.stats_games
      , s.stats_position
      , s.stats_at_bat
      , s.stats_wins
      , s.stats_losses
      , s.stats_runs
      , s.stats_hits
      , s.stats_doubles
      , s.stats_triples
      , s.stats_home_runs
      , s.stats_rbi
      , s.stats_stolen_bases
      , s.stats_hit_by_pitch
      , s.stats_sac_hits
      , s.stats_sac_flies
      , s.stats_grd_dbl_play
      , s.stats_putouts
      , s.stats_assists
      , s.stats_errors
      , s.stats_dbl_plays
      , s.stats_passed_balls_c
      , s.stats_wild_pitches
      , s.stats_opp_stolen_bases_c
      , s.stats_strike_outs
      , s.stats_opp_batting_avg
      , s.stats_games_started
      , s.stats_completed_games
      , s.stats_shutouts
      , s.stats_saves
      , s.stats_era
      , s.stats_intentional_walk
    FROM player_stats_by_year as s
      LEFT JOIN people as p ON s.player_id = p.playerID
    WHERE
      (@year_id IS NULL OR (s.stats_year = @year_id))
      AND (@player_first_name IS NULL OR p.nameFirst LIKE '%' + @player_first_name + '%')
      AND (@player_last_name IS NULL OR p.nameLast LIKE '%' + @player_last_name + '%')
      AND ((@pitchers = 1 AND s.stats_position = 'P') OR (@fielders = 1 AND s.stats_position != 'P'))
    SET @rowcount = @@ROWCOUNT
    IF (@rowcount > 500)
    BEGIN
      RAISERROR ('More than 500 rows returned, please refine your search!', 16, 1)
    RETURN
    END
  END
END
GO


/*
Procedure execution format:

EXEC get_player_stats @year_id = (Number or NULL), @player_first_name = (String or NULL), @player_last_name = (String or NULL), @pitchers = (0 or 1), @fielders = (0 or 1), @personal = (0 or 1), @professional = (0 or 1)

Variables will default to NULL or 0 unless otherwise specified.  Variables with options 0 or 1 act as Boolean operators where 0 = False and 1 = True.

Statements that return more than 500 rows will prompt users to refine their search.
*/

--Example #1: returns the personal stats for all fielders with the first name 'reggie' for all years.
EXEC get_player_stats @year_id = NULL, @player_first_name = 'reggie', @player_last_name = NULL, @pitchers = 0, @fielders = 1, @personal = 1, @professional = 0

--Example #2: returns the professional stats for all pitchers in the year 1974.
EXEC get_player_stats @year_id = 1974, @player_first_name = NULL, @player_last_name = NULL, @pitchers = 1, @fielders = 0, @personal = 0, @professional = 1

--Example #3: throws error because too many rows are returned.
EXEC get_player_stats @year_id = NULL, @player_first_name = NULL, @player_last_name = NULL, @pitchers = 0, @fielders = 1, @personal = 0, @professional = 1

---- Ryan Summers Stored Procedure Use Case 2:
drop procedure if exists get_fielder_stats
GO
create procedure get_fielder_stats
    @year int = NULL,
    @min_batting_avg float = null,
    @min_hr int = null,
    @min_hits int = null,
    @min_obp float = null,
    @min_games_played int = null

AS
BEGIN
    set nocount on

    select 
        p.player_name, p.player_firstname, p.player_middlename, p.player_lastname, p.player_nickname, p.player_url,
        s.stats_year, 
        cast(s.stats_hits as float) / nullif(s.stats_at_bat, 0) as batting_avg,
        s.stats_home_runs, s.stats_hits,
        cast(s.stats_hits+s.stats_base_on_balls+s.stats_intentional_walk+s.stats_hit_by_pitch as float) /
            nullif(s.stats_at_bat+s.stats_base_on_balls+s.stats_intentional_walk+s.stats_hit_by_pitch+s.stats_sac_flies,0) as on_base_perc,
        s.stats_games
    from players as p
        inner join player_stats_by_year as s 
            on p.player_id = s.players_player_id 
    where s.stats_position != 'P'
        and (@year is null or s.stats_year = @year)
        and (@min_batting_avg is null or cast(s.stats_hits as float) / nullif(s.stats_at_bat,0) >= @min_batting_avg)
        and (@min_hr is null or s.stats_home_runs >= @min_hr)
        and (@min_hits is null or s.stats_hits >= @min_hits)
        and (@min_obp is null or cast(s.stats_hits+s.stats_base_on_balls+s.stats_intentional_walk+s.stats_hit_by_pitch as float) /
        nullif(s.stats_at_bat+s.stats_base_on_balls+s.stats_intentional_walk+s.stats_hit_by_pitch+s.stats_sac_flies,0) >= @min_obp)
        and (@min_games_played is null or s.stats_games >= @min_games_played)
    order by s.stats_year, p.player_name
END
GO

exec get_fielder_stats 
    @year = 1982
    ,@min_batting_avg = 0.25 
    ,@min_hr = 1 
    ,@min_hits = 1 
    ,@min_obp = 0.30 
    ,@min_games_played = 50

-- END USE CASE #2

---- Ryan Summers Stored Procedure Use Case 3:
drop procedure if exists get_pitcher_stats
GO
create procedure get_pitcher_stats
    @year int = NULL,
    @min_era float = null,
    @min_strk_out int = null,
    @min_wins int = null,
    @min_inns_pitched int = null

AS
BEGIN
    set nocount on

    select p.player_name, p.player_firstname, p.player_middlename, p.player_lastname,
           p.player_nickname, p.player_url,
           s.stats_year, s.stats_era, s.stats_strike_outs, s.stats_wins,
           s.stats_outs_pitched
    from players as p
        inner join player_stats_by_year as s 
            on p.player_id = s.players_player_id 
    where s.stats_position = 'P'
        and (@year is null or s.stats_year = @year)
        and (@min_era is null or s.stats_era >= @min_era)
        and (@min_strk_out is null or s.stats_strike_outs >= @min_strk_out)
        and (@min_wins is null or s.stats_wins >= @min_wins)
        and (@min_inns_pitched is null or s.stats_outs_pitched >= @min_inns_pitched)
    order by s.stats_year, p.player_name
END

exec get_pitcher_stats 
    @year = 1977 
    ,@min_era = 6 
    ,@min_strk_out = 3.50 
    ,@min_wins = 1.20 
    ,@min_inns_pitched = 100

-- END USE CASE #3

-- USE CASE #4 - Card Select - 6 scenarios addressed
/*
Scenarios:	Return Type	Card Search Type	Action	
1	Graded cards	Pitchers	Select ONLY cards from bills_cards.  Include graded data and join in pitcher stats	
2	Graded cards	Fielders	Select ONLY cards from bills_cards.  Include graded data and join in fielder stats	
3	Graded cards	Other	Select ONLY cards from bills_cards.  Include only graded data	
4	Raw cards	Pitchers	Select cards from "cards" table.  Join in pitcher stats	
5	Raw cards	Fielders	Select cards from "cards" table.  Join in fielder stats	
6	Raw cards	Other	select cards from "cards" table.  Include only data from that table.	
*/

drop procedure if exists p_card_select_pitchers
drop procedure if exists p_card_select_fielders
drop procedure if exists p_card_select_other

GO
Create procedure p_card_select_fielders
    @card_type char(6), --raw or graded
    @search_str nvarchar(100), -- serach string
    @year char(4) --specific year or 'all' for all years
AS           
IF @year = 'all' --for all years
    BEGIN
        IF @card_type='raw' --raw cards
            BEGIN
            select c.card_desc,c.card_year,c.card_num,c.card_stat_year,c.card_notes --from cards
                    ,p.stats_position,p.stats_batting_avg,p.stats_on_base_perc,p.stats_at_bat,p.stats_hits--key stats for fielders
                    ,p.stats_doubles,p.stats_triples,p.stats_home_runs,p.stats_rbi--key stats for fielders (continued)
                    ,t.team_city,t.team_name,t.team_league --team information by year
            from cards c 
                join player_stats_by_year p on c.card_player_id=p.players_player_id 
                join teams t on t.team_code=p.team_id
            where c.card_desc like @search_str and c.card_stat_year=p.stats_year order by c.card_year
            END
        ELSE --graded cards
            BEGIN
            select c.bills_card_psa_desc,c.bills_card_cert,c.bills_card_num,c.bills_card_year,c.bills_card_grade,c.bills_card_pop,c.bills_card_pop_higher from bills_cards c 
                join player_stats_by_year p on c.bills_card_player_id=p.players_player_id
                join teams t on t.team_code=p.team_id 
            where c.bills_card_psa_desc like @search_str and c.bills_card_stat_year=p.stats_year order by c.bills_card_year
            END
    END
    ELSE --for a specific year
    BEGIN
        IF @card_type='raw' --for raw cards
            BEGIN
            select c.card_desc,c.card_year,c.card_num,c.card_stat_year,c.card_notes --from cards
                    ,p.stats_position,p.stats_batting_avg,p.stats_on_base_perc,p.stats_at_bat,p.stats_hits--key stats for fielders
                    ,p.stats_doubles,p.stats_triples,p.stats_home_runs,p.stats_rbi--key stats for fielders (continued)
                    ,t.team_city,t.team_name,t.team_league --team information by year
            from cards c 
                join player_stats_by_year p on c.card_player_id=p.players_player_id 
                join teams t on t.team_code=p.team_id
            where c.card_desc like @search_str and c.card_year like @year and c.card_stat_year=p.stats_year order by c.card_year
            END
        ELSE --for graded cards
            BEGIN
            select c.bills_card_psa_desc,c.bills_card_cert,c.bills_card_num,c.bills_card_year,c.bills_card_grade,c.bills_card_pop,c.bills_card_pop_higher from bills_cards c 
                join player_stats_by_year p on c.bills_card_player_id=p.players_player_id 
                join teams t on t.team_code=p.team_id
            where c.bills_card_psa_desc like @search_str and c.bills_card_year like @year and c.bills_card_stat_year=p.stats_year order by c.bills_card_year
            END
    END

GO
Create procedure p_card_select_pitchers
    @card_type char(6), --raw or graded
    @search_str nvarchar(100), -- serach string
    @year char(4) --specific year or 'all' for all years
AS           
IF @year = 'all' --for all years
    BEGIN
        IF @card_type='raw' --raw cards
            BEGIN
            select c.card_desc,c.card_year,c.card_num,c.card_stat_year,c.card_notes --from cards
                    ,p.stats_wins,p.stats_losses,p.stats_era--key stats for pitchers
                    ,t.team_city,t.team_name,t.team_league --team information by year
            from cards c 
                join player_stats_by_year p on c.card_player_id=p.players_player_id 
                join teams t on t.team_code=p.team_id
            where c.card_desc like @search_str and c.card_stat_year=p.stats_year order by c.card_year
            END
        ELSE --graded cards
            BEGIN
            select c.bills_card_psa_desc,c.bills_card_cert,c.bills_card_num,c.bills_card_year,c.bills_card_grade,c.bills_card_pop,c.bills_card_pop_higher from bills_cards c 
                join player_stats_by_year p on c.bills_card_player_id=p.players_player_id
                join teams t on t.team_code=p.team_id 
            where c.bills_card_psa_desc like @search_str and c.bills_card_stat_year=p.stats_year order by c.bills_card_year
            END
    END
    ELSE --for a specific year
    BEGIN
        IF @card_type='raw' --for raw cards
            BEGIN
            select c.card_desc,c.card_year,c.card_num,c.card_stat_year,c.card_notes --from cards
                    ,p.stats_wins,p.stats_losses,p.stats_era--key stats for pitchers
                    ,t.team_city,t.team_name,t.team_league --team information by year
            from cards c 
                join player_stats_by_year p on c.card_player_id=p.players_player_id 
                join teams t on t.team_code=p.team_id
            where c.card_desc like @search_str and c.card_year like @year and c.card_stat_year=p.stats_year order by c.card_year
            END
        ELSE --for graded cards
            BEGIN
            select c.bills_card_psa_desc,c.bills_card_cert,c.bills_card_num,c.bills_card_year,c.bills_card_grade,c.bills_card_pop,c.bills_card_pop_higher from bills_cards c 
                join player_stats_by_year p on c.bills_card_player_id=p.players_player_id 
                join teams t on t.team_code=p.team_id
            where c.bills_card_psa_desc like @search_str and c.bills_card_year like @year and c.bills_card_stat_year=p.stats_year order by c.bills_card_year
            END
    END

GO
Create procedure p_card_select_other
    @card_type char(6), --raw or graded
    @search_str nvarchar(100), -- serach string
    @year char(4) --specific year or 'all' for all years
AS           
IF @year = 'all' --for all years
    BEGIN
        IF @card_type='raw' --raw cards
            BEGIN
                select c.card_desc,c.card_year,c.card_num,c.card_notes --from cards
                from cards c 
                where c.card_desc like @search_str and c.card_player_id is null order by c.card_year
            END
        ELSE --graded cards
            BEGIN
                select c.bills_card_psa_desc,c.bills_card_cert,c.bills_card_num,c.bills_card_year,c.bills_card_grade,c.bills_card_pop,c.bills_card_pop_higher 
                from bills_cards c 
                where c.bills_card_psa_desc like @search_str and c.bills_card_player_id is null order by c.bills_card_year
            END
    END
    ELSE --for a specific year
    BEGIN
        IF @card_type='raw' --for raw cards
            BEGIN
                select c.card_desc,c.card_year,c.card_num,c.card_notes --from cards
                from cards c 
                where c.card_desc like @search_str and c.card_year like @year and c.card_player_id is null
            END
        ELSE --for graded cards
            BEGIN
                select c.bills_card_psa_desc,c.bills_card_cert,c.bills_card_num,c.bills_card_year,c.bills_card_grade,c.bills_card_pop,c.bills_card_pop_higher 
                from bills_cards c 
                where c.bills_card_psa_desc like @search_str and c.bills_card_year like @year and c.bills_card_player_id is null
            END
    END

GO
declare @card_search_type char(8) -- 3 options from application 'pitchers', 'fielders','other'
set @card_search_type='fielders' --simulating as a variable sent from the User interface

IF @card_search_type='pitchers'--simulating inputs as variables from the user interface
    BEGIN
        EXEC p_card_select_pitchers
            @card_type='raw',
            @search_str='nol% rya%',
            @year='all'
    END
ELSE IF @card_search_type='fielders'
    BEGIN
        EXEC p_card_select_fielders
            @card_type='raw',
            @search_str='regg% jack%',
            @year='all'
    END
ELSE
    BEGIN
        EXEC p_card_select_other
            @card_type='raw',
            @search_str='rookie%',
            @year='all'
    END

--TEMPORTY CODE THAT WAS USED TO CHECK RESULTS
/*select c.bills_card_player_id,p.players_player_id,bills_card_year from bills_cards c 
join player_stats_by_year p on c.bills_card_player_id=p.players_player_id where c.bills_card_player_id != p.players_player_id

select bills_card_cert,bills_card_psa_desc,bills_card_player_id,bills_card_year from bills_cards 
where bills_card_player_id is not null order by bills_card_psa_desc

select * from players where player_id = 1270

declare @player_id INT
set @player_id = (select player_id from players where player_name like 'Regg% Jack%')
select * from player_stats_by_year p 
    join teams t on p.team_id=t.team_code 
    join players pl on p.players_player_id=pl.player_id where players_player_id =@player_id order by stats_year*/

--END USE CASE 4