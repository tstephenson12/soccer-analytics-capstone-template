import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

test_query = duckdb.sql(f"""
                        SELECT match_id, team_id, period, start_minutes, start_seconds, COUNT(*)
                        FROM (
                              SELECT pl.*, player_id, position_type,
                              CASE WHEN position_type = 'M' THEN 1 ELSE 0 END AS MIDFIELDERS,
                              CASE WHEN position_type = 'B' THEN 1 ELSE 0 END AS BACKS,
                              CASE WHEN position_type = 'F' THEN 1 ELSE 0 END AS FORWARDS,
                              CASE WHEN position_type = 'F' AND position_type_alt = 'CF' THEN 1 ELSE 0 END AS CENTER_FORWARDS,
                              CASE WHEN position_type = 'M' AND POSITION_BEHAVIOR = 'A' THEN 1 ELSE 0 END AS ATTACKING_MIDFIELDERS,
                              CASE WHEN position_type = 'M' AND POSITION_BEHAVIOR = 'D' THEN 1 ELSE 0 END AS DEFENDING_MIDFIELDERS
                              FROM read_parquet('{project_location}/eda/period_lineups.parquet')  pl
                              LEFT JOIN read_parquet('{project_location}/eda/position_type.parquet') pt
                                 ON pl.position_name = pt.position_name
                          )
                          GROUP BY match_id, team_id, period, start_minutes, start_seconds
                          HAVING COUNT(*) < 10

                    """)

print(test_query)


test_query2 = duckdb.sql(f"""
                         
                        with all_lineup_times as (
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
                                 
                                 
                                 )
                            )
                        SELECT *
                        FROM all_lineup_times
                        WHERE match_id = 3795109 AND team_id = 912


                    """).write_csv('test_lineup.csv')

print(test_query2)
test_query2 = duckdb.sql(f"""

                      SELECT 1 period, 99999999 AS minutes, 99999999 AS seconds 
                         
                         UNION

                      SELECT 2 period, 99999999 AS minutes, 99999999 AS seconds 


                     """)#.write_csv('test_lineup.csv')

print(test_query2)


test_query3 = duckdb.sql(f"""

                              with player_match_info as (
                           SELECT  *
                              FROM read_parquet('{project_location}/eda/period_lineups.parquet') 
                              WHERE match_id = 7582 
                              ),
                              match_info as (
                              SELECT distinct match_id, team_id, period, start_minutes, start_seconds
                              FROM player_match_info
                              )
                              SELECT mi.match_id, mi.team_id, mi.period, mi.start_minutes, mi.start_seconds, COUNT(*)
                              FROM match_info mi
                              LEFT JOIN player_match_info p
                                 ON mi.match_id = p.match_id
                                 AND mi.team_id = p.team_id
                                 AND mi.period = p.period
                                 AND (
                                      (mi.start_minutes = IFNULL(p.end_minutes,9999999) AND mi.start_seconds < IFNULL(p.end_seconds,9999999))
                                      OR 
                                      (mi.start_minutes > p.start_minutes AND mi.start_minutes < IFNULL(p.end_minutes,9999999))
                                      OR
                                      (mi.start_minutes = p.start_minutes AND mi.start_seconds >= p.start_seconds AND mi.start_minutes != IFNULL(p.end_minutes,9999999))
                                      )
                              LEFT JOIN read_parquet('{project_location}/eda/position_type.parquet') pt
                                 ON p.position_name = pt.position_name
                              GROUP BY mi.match_id, mi.team_id, mi.period, mi.start_minutes, mi.start_seconds
                              ORDER BY mi.match_id, mi.team_id, mi.period, mi.start_minutes, mi.start_seconds


                    """)

print(test_query3)
