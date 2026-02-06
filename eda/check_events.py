import duckdb
#import pandas as pd
import polars as pl
#import altair as alt ###Installed 2/6 uninstalled after trying

import plotly.express as px
import plotly #installed kaleido

import plotly.graph_objects as go


project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'

con = duckdb.connect()


soccer_360 = duckdb.sql(f"""
                        SELECT *
                        FROM read_parquet('{project_location}/Statsbomb/three_sixty.parquet') 
                        WHERE match_id IN (
                            SELECT match_id
                            FROM read_parquet('{project_location}/Statsbomb/matches.parquet') 
                            WHERE season = '2024' AND match_status_360 = 'available'
                        )
                            """)

#print(soccer_360)
#print(soccer_360.columns)

all_coordinates = duckdb.sql(f"""
                        SELECT e.*, ts.teammate, ts.actor, ts.keeper, ts.location_x player_location_x, ts.location_y player_location_y
                        FROM (
                        SELECT id, match_id, index_num, period, timestamp, location_x event_location_x, location_y event_location_y
                        FROM read_parquet('{project_location}/Statsbomb/events.parquet') 
                        WHERE match_id = '3930171' AND location_x IS NOT NULL AND location_y IS NOT NULL
                        ) e
                        LEFT JOIN read_parquet('{project_location}/Statsbomb/three_sixty.parquet') ts
                            ON e.match_id = ts.match_id
                            AND e.id = ts.event_uuid
                        ORDER BY index_num
                            """)

#print(all_coordinates)
#print(all_coordinates.columns)

sub_coordinates = duckdb.sql(f"""
                        SELECT e.*, ts.teammate, ts.actor, ts.keeper, ts.location_x player_location_x, ts.location_y player_location_y
                        FROM (
                        SELECT id, match_id, index_num, period, timestamp, location_x event_location_x, location_y event_location_y
                        FROM read_parquet('{project_location}/Statsbomb/events.parquet') 
                        WHERE match_id = '3930171' AND location_x IS NOT NULL AND location_y IS NOT NULL
                        ) e
                        LEFT JOIN read_parquet('{project_location}/Statsbomb/three_sixty.parquet') ts
                            ON e.match_id = ts.match_id
                            AND e.id = ts.event_uuid
                        WHERE index_num = 5
                            """)



sub_coordinates = duckdb.sql(f"""
                        
                        LOAD spatial;
                             
                        SELECT rank_dist.*, 
                             CASE 
                             WHEN actor = TRUE THEN 'Ball'
                             WHEN keeper = TRUE AND teammate = TRUE THEN 'Keeper - Same Team'
                             WHEN keeper = TRUE AND teammate = FALSE THEN 'Keeper - Opposing Team'
                             WHEN teammate = TRUE AND proximity_rank <= 3 THEN 'Closest 3 - Same Team'
                             WHEN teammate = FALSE AND proximity_rank <= 3 THEN 'Closest 3 - Opposing Team'
                             WHEN teammate = TRUE AND proximity_rank > 3 THEN 'Same Team'
                             WHEN teammate = FALSE AND proximity_rank > 3 THEN 'Opposing Team'
                             ELSE NULL
                             END AS player_category
                        FROM (
                        SELECT dist_to_event.*, RANK() OVER (PARTITION BY match_id, index_num, actor, keeper, teammate ORDER BY match_id, index_num, actor, keeper, teammate, dist_to_event) proximity_rank
                        FROM (     
                        SELECT e.*, ts.teammate, ts.actor, ts.keeper, ts.location_x player_location_x, ts.location_y player_location_y, ST_Distance(ST_Point(ts.location_x, ts.location_y), ST_Point(event_location_x, event_location_y)) dist_to_event
                        FROM (
                        SELECT id, match_id, index_num, period, timestamp, location_x event_location_x, location_y event_location_y
                        FROM read_parquet('{project_location}/Statsbomb/events.parquet') 
                        WHERE match_id = '3930171' AND location_x IS NOT NULL AND location_y IS NOT NULL
                        ) e
                        LEFT JOIN read_parquet('{project_location}/Statsbomb/three_sixty.parquet') ts
                            ON e.match_id = ts.match_id
                            AND e.id = ts.event_uuid
                        WHERE index_num = 5 
                        ) dist_to_event
                        ) rank_dist
                            """)

print(sub_coordinates)


soccer_df = pl.DataFrame(sub_coordinates)

soccer_fig = px.scatter(soccer_df, x="player_location_x", y="player_location_y", color="player_category")
soccer_fig.add_shape(
    type='line', x0=60.10404, y0=50.18, x1=67.94788, y1=48.3968,
    line=dict(color='red', width=1, dash='dot')
)
soccer_fig.add_shape(
    type='line', x0=60.10404, y0=50.18, x1=59.563877, y1=28.2041,
    line=dict(color='red', width=1, dash='dot')
)
soccer_fig.add_shape(
    type='line', x0=67.94788, y0=48.3968, x1=59.563877, y1=28.2041,
    line=dict(color='red', width=1, dash='dot')
)


plotly.io.write_image(soccer_fig, 'test.pdf', format='pdf')





