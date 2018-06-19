"""
Created on Wed Jun 13 10:55:06 2018
@author: SRanganath
,,,"""

import pandas as pd
import pyodbc
import datetime
import re
import matplotlib.pyplot as plt

'''Connection String and Query to fetch  data'''
import Connections.py

conn = pyodbc.connect("""Driver={SQL Server Native Client 11.0};
                      Server=172.21.128.193; "
                      "Database=Wide_Orbit; UID=etl_user_prod;
                      pwd=T@gna2018;""")
query = '''SELECT * FROM [Wide_Orbit].[dbo].[buyer_demand_fcst_input_new2]
         where [Market] = \'Atlanta\''''

df = pd.read_sql(query, conn)

df = df.sort_values(by=['air_year', 'air_week', 'program_start_time'])

'''date_correction'''

df['prg_strt_time'] = [datetime.datetime.time(d) for
    d in df['program_start_time']]

df['prg_end_time'] = [datetime.datetime.time(d) for
    d in df['program_end_time']]

'''rearranging the cleaned columns'''
df = df[['market', 'Station_Name', 'daypart_name', 'invcode_name',
         'invcode_External_Id', 'break_code', 'full_date', 'order_number',
         'order_created', 'program_start_time', 'prg_strt_time',
         'program_end_time', 'prg_end_time', 'potential_units', 'spot_counts',
         'spot_rating_key', 'affiliation', 'gross_revenue', 'air_year',
         'air_week', 'order_year', 'Order_week', 'full_datetime',
         'created_datetime', 'last_updated']]

'''Treating null values, removing SP dayparts with olympics'''

sp_df = df[(df.daypart_name == 'SP')]

p = re.compile('.*(olympic).*', re.IGNORECASE)
olympics_df = df[(df.daypart_name == 'SP') & df.invcode_name.str.match(p)]

null_df = df[df.program_start_time != df.program_start_time]

olympics_df = df[(df.daypart_name == 'SP') & df.invcode_name.str.match(p)]

u_inv = list((sp_df.invcode_name.unique()))

sports_df = df[(df.daypart_name == 'SP') & (df.invcode_name != (p))]
sports_df2 = sports_df[(~sports_df.invcode_name.str.match(p))]

