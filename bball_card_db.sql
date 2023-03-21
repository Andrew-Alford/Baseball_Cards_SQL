
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
    stats_position varchar(2) not null,
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

--TABLE PLAYERS...Loaded data into players from a .csv using the Azure extension and then altered it to add an ID ad a PK
Alter Table players
    Add player_id Int Identity(1, 1)
Alter Table players
    Add primary key (player_id)
Alter Table players
    drop column player_id_load

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

select card_num,card_desc,card_notes,card_year,card_player_url from cards where card_desc like '%egg% Jack%'
select * from people
select * from bills_cards
select * from cards
select * from players

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