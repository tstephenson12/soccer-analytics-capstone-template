import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

duckdb.sql(f"""
                            with half_timestamps as (
                              SELECT distinct match_id, team_id, period, minute, second, timestamp
                                    FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
                                    WHERE type IN ('Half End', 'Half Start')
                                    
                         ),

                        all_lineup_times as (
                           SELECT distinct match_id, team_id, period_tracked, 
                           CAST(LEFT(period_time, INSTR(period_time, ':') - 1 ) AS INTEGER)  period_minute, 
                           CAST(SUBSTR(period_time, LEN(period_time) - 1) AS INTEGER) period_second
                           FROM (SELECT distinct match_id, team_id, from_time period_time, from_period period_tracked
                                 FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                                 WHERE from_period IS NOT NULL AND from_time IS NOT NULL AND IFNULL(from_time,'N/A') != IFNULL(to_time,'N/A')

                                 UNION
                                 
                                 SELECT distinct match_id, team_id, to_time, to_period
                                 FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                                 WHERE to_period IS NOT NULL AND to_time IS NOT NULL AND IFNULL(from_time,'N/A') != IFNULL(to_time,'N/A')

                                 UNION

                                 SELECT distinct match_id, team_id, timestamp, period
                                 FROM half_timestamps
                                 
                                 )
                            ),

                              match_timeline as (
                                 SELECT *
                                 FROM (
                                       SELECT match_id, team_id, period_tracked period, period_minute start_minutes, period_second start_seconds, 
                                             IFNULL(LEAD(period_minute,1) OVER (PARTITION BY match_id, team_id, period_tracked ORDER BY match_id, team_id, period_tracked, period_minute, period_second),999999999) end_minutes, 
                                             IFNULL(LEAD(period_second,1) OVER (PARTITION BY match_id, team_id, period_tracked ORDER BY match_id, team_id, period_tracked, period_minute, period_second),999999999) end_seconds 
                                       FROM all_lineup_times
                                       )
                                 WHERE period IS NOT NULL 
                                  AND NOT (start_minutes = end_minutes AND start_seconds = end_seconds)
                              ),
                              all_lineup_changes as (
                              SELECT check_players.match_id, team_id, player_id, country_id, country_name, from_period, IFNULL(to_period, last_period) to_period, 
                              position_name,
                              CAST(LEFT(from_time, INSTR(from_time, ':') - 1 ) as INTEGER) from_time_minutes, 
                              CAST(SUBSTR(from_time, LEN(from_time) - 1) AS INTEGER) from_time_seconds,
                              IFNULL( CAST(LEFT(to_time, INSTR(to_time, ':') - 1 ) as INTEGER),9999999) to_time_minutes, 
                              IFNULL( CAST(SUBSTR(to_time, LEN(to_time) - 1) AS INTEGER) ,9999999) to_time_seconds
                              FROM (SELECT * 
                                    FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                                    WHERE from_period IS NOT NULL AND IFNULL(from_time,'N/A') != IFNULL(to_time,'N/A')) check_players
                              LEFT JOIN (
                                          SELECT match_id, MAX(period) last_period
                                          FROM read_parquet('{project_location}/data/Statsbomb/events.parquet')
                                          GROUP BY match_id) last_period
                              ON check_players.match_id = last_period.match_id
                               
                           ),
                           period_start_end as (
                           SELECT mt2.match_id, mt2.period, 
                           MIN(start_minutes) period_start_minute, MIN(start_seconds) period_start_second, 
                           MAX(end_minutes) period_end_minute, MAX(end_seconds) period_end_second
                           FROM match_timeline mt2
                           GROUP BY mt2.match_id, mt2.period
                           ),
                           all_lineup_events as (
                           SELECT distinct mt3.match_id, mt3.team_id, mt3.period, player_id, country_id, position_name, country_name, mt3.start_minutes, mt3.start_seconds, mt3.end_minutes, mt3.end_seconds
                           --from_period, to_period
                           --from_time_minutes, from_time_seconds, to_time_minutes, to_time_seconds
                           FROM match_timeline mt3
                           INNER JOIN all_lineup_changes c
                              ON mt3.match_id = c.match_id
                              AND mt3.team_id = c.team_id
                              AND mt3.period > c.from_period AND mt3.period < c.to_period 
                                     
                          UNION


                           SELECT distinct mt4.match_id, mt4.team_id, mt4.period, player_id, country_id, position_name, country_name, mt4.start_minutes, mt4.start_seconds, mt4.end_minutes, mt4.end_seconds
                           --from_period, to_period
                           --from_time_minutes, from_time_seconds, to_time_minutes, to_time_seconds
                           FROM match_timeline mt4
                           INNER JOIN all_lineup_changes c2
                              ON mt4.match_id = c2.match_id
                              AND mt4.team_id = c2.team_id
                              AND mt4.period = c2.from_period
                              AND ((start_minutes > from_time_minutes AND start_minutes < to_time_minutes )

                              OR 
                                    (start_minutes = from_time_minutes AND start_seconds >= from_time_seconds AND start_seconds < to_time_seconds)
                                    
                              
                              )




                        UNION


                           SELECT distinct mt5.match_id, mt5.team_id, mt5.period, player_id, country_id, position_name, country_name, mt5.start_minutes, mt5.start_seconds, mt5.end_minutes, mt5.end_seconds
                           --from_period, to_period
                           --from_time_minutes, from_time_seconds, to_time_minutes, to_time_seconds
                           FROM match_timeline mt5
                           INNER JOIN all_lineup_changes c3
                              ON mt5.match_id = c3.match_id
                              AND mt5.team_id = c3.team_id
                              AND mt5.period = c3.to_period
                              AND c3.from_period != c3.to_period 
                              AND ((start_minutes > from_time_minutes AND start_minutes < to_time_minutes )

                              OR 
                                    (start_minutes = from_time_minutes AND start_seconds >= from_time_seconds AND start_seconds < to_time_seconds)
                                    
                              
                              )

                           ),
                           adj_player_times as (
                           SELECT distinct mt_final.match_id, mt_final.team_id, player_id, country_id, position_name, mt_final.period, mt_final.start_minutes, mt_final.start_seconds, mt_final.end_minutes, mt_final.end_seconds
                           FROM match_timeline mt_final
                           LEFT JOIN all_lineup_events al
                            ON mt_final.match_id = al.match_id
                            AND mt_final.team_id = al.team_id
                            AND mt_final.period = al.period
                            AND (mt_final.start_minutes >= al.start_minutes 
                                      AND mt_final.start_seconds >= al.start_seconds
                                      AND (mt_final.start_minutes <= al.end_minutes  
                                                OR mt_final.start_seconds < al.end_seconds)
                                      )
                           )
                           
                           SELECT *
                           FROM adj_player_times

                    """).write_parquet('period_lineups.parquet')
