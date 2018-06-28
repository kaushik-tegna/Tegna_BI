# -*- coding: utf-8 -*-
"""
Created on Tue Jun 26 15:21:23 2018

@author: SRanganath
"""
# import time
from Connections import WIDE_ORBIT_CONNECT, WIDE_ORBIT_QUERY
import pandas as pd
# import numpy as np
import datetime
import re
# import time
# import statsmodels
from statsmodels.robust.scale import mad
# start = time.time()
# end = time.time()
# print(end - start)

# Connection String and Query to fetch  data
Current_Year = datetime.datetime.now().year
TS_Years = list(pd.Series(range((Current_Year-4), (1+Current_Year))))

data = pd.read_sql(WIDE_ORBIT_QUERY, WIDE_ORBIT_CONNECT)
data = data.sort_values(by=['air_year', 'air_week', 'program_start_time'])

# date_correction

data['start_time_cleaned'] = [datetime.datetime.time(time) for time
                              in data['program_start_time']]
data['end_time_cleaned'] = [datetime.datetime.time(time) for time
                            in data['program_end_time']]

# rearranging the cleaned columns

data = data[['market', 'Station_Name', 'daypart_name', 'invcode_name',
             'invcode_External_Id', 'break_code', 'full_date', 'order_number',
             'order_created', 'program_start_time', 'start_time_cleaned',
             'program_end_time', 'end_time_cleaned', 'potential_units',
             'spot_counts', 'spot_rating_key', 'affiliation', 'gross_revenue',
             'air_year', 'air_week', 'order_year', 'Order_week',
             'full_datetime', 'created_datetime', 'last_updated']]

# Treating null values, removing SP dayparts with olympics

SP_data = data[(data.daypart_name == 'SP')]

# Olympics only data
olympic_pattern = re.compile('.*(olympic).*', re.IGNORECASE)
Olympics_data = data[(data.daypart_name == 'SP') &
                     data.invcode_name.str.match(olympic_pattern)]

# Creating a dataframe with only sports data

Sports_data = SP_data[(~SP_data.invcode_name.str.match(olympic_pattern))]

# Grouping for TS Data

TS_groups = data.groupby(['market', 'Station_Name', 'daypart_name',
                          'invcode_name', 'air_week'])['air_year'].unique()
TS_groups = TS_groups.apply(list)
TS_groups = TS_groups.to_frame().reset_index()


# test_df.air_year.dtype

# Merging the data frame with the groupby object

TS_data = pd.merge(TS_groups, data,  how='left',
                   on=['market', 'Station_Name', 'daypart_name',
                       'invcode_name', 'air_week'])

# Determining the amount of historical data for time series
# Current_Year = datetime.datetime.now().year
# TS_Years = list(pd.Series(range((Current_Year-4), (1+Current_Year))))

# Function to check wether rows are fit for Time Series


def TS_check(x):
    global TS_Years
    if set(x) & set(TS_Years) == set(TS_Years):
        result = 'Fit'
    else:
        result = 'Unfit'
    return result


TS_data['time_series_flag'] = TS_data['air_year_x'].apply(TS_check)

# view2 = data.head(1000)
# view = Median_data.head(1000)
# Restructuring the dataframe

data = TS_data.drop(columns=['air_year_x'])
data = data.rename(index=str, columns={'air_year_y': 'air_year'})
data = data.rename(str.lower, axis='columns')

del TS_data, TS_groups, Current_Year, TS_Years

# Calculating the grouped median

Median_group = data.groupby(['market', 'station_name',
                             'daypart_name', 'invcode_name',
                             'air_week', 'air_year'])['spot_counts'].median()
Median_group = Median_group.reset_index()
Median_data = pd.merge(Median_group, data, how='left',
                       on=['market', 'station_name', 'daypart_name',
                           'invcode_name', 'air_week', 'air_year'])
Median_data = Median_data.rename(index=str,
                                 columns={'spot_counts_y': 'spot_counts',
                                          'spot_counts_x': 'spot_median'})
del Median_group


# Calculating the Median absolute deviation

mad_group = Median_data.groupby(['market', 'station_name',
                                 'daypart_name', 'invcode_name',
                                 'air_week', 'air_year'])['spot_counts'].mad()
mad_group = mad_group.reset_index()

Mad_data = pd.merge(mad_group, Median_data, how='left',
                    on=['market', 'station_name', 'daypart_name',
                        'invcode_name', 'air_week', 'air_year'])
Mad_data = Mad_data.rename(index=str,
                           columns={'spot_counts_y': 'spot_counts',
                                    'spot_counts_x': 'spot_mad'})

view = Mad_data.head(1000)

View1 = Mad_data[['spot_counts', 'spot_mad', 'spot_median']]
