"""
Created on Wed Jun 13 10:55:06 2018
@author: SRanganath
,,,"""
from Connections import WIDE_ORBIT_CONNECT, WIDE_ORBIT_QUERY
import pandas as pd
import datetime
import re

"""import matplotlib.pyplot as plt"""

'''Connection String and Query to fetch  data'''
df = pd.read_sql(WIDE_ORBIT_QUERY, WIDE_ORBIT_CONNECT)

df = df.sort_values(by=['air_year', 'air_week', 'program_start_time'])

'''date_correction'''

df['prg_strt_time'] = [datetime.datetime.time(d) for d
                       in df['program_start_time']]

df['prg_end_time'] = [datetime.datetime.time(d) for d
                      in df['program_end_time']]

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


'''Olympics only data'''
olympic_pattern = re.compile('.*(olympic).*', re.IGNORECASE)
olympics_df = df[(df.daypart_name == 'SP') &
                 df.invcode_name.str.match(olympic_pattern)]

#null_df = df[df.program_start_time != df.program_start_time]
#
#u_inv = list((sp_df.invcode_name.unique()))
'''Creating a dataframe with only sports data'''
sports_df = sp_df[(~sp_df.invcode_name.str.match(olympic_pattern))]

'''Identifying shows with missing years'''

view = sp_df
inv = list(df.invcode_name.unique())

'''Creating a dictionary with years data'''
years_dict = {}
for invcode_name in inv:
    temp = df[(df.invcode_name == invcode_name)]
    years_dict[invcode_name] = temp.air_year.unique()
    del temp
