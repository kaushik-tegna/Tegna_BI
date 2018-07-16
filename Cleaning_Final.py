# -*- coding: utf-8 -*-
"""
Created on Thu Jul 12 13:22:23 2018

@author: SRanganath
"""
from Connections import WIDE_ORBIT_CONNECT, WIDE_ORBIT_QUERY, VALIDATION_QUERY
import Clean_Fx
import pandas as pd
import datetime
# import matplotlib.pyplot as plt
# import numpy as np
# import re
# import time
# import statsmodels
# start = time.time()

# Time calculations
current_year = datetime.datetime.now().year
ts_years = list(pd.Series(range((current_year-3), (1+current_year))))


data = pd.read_sql(WIDE_ORBIT_QUERY, WIDE_ORBIT_CONNECT)
data = data.sort_values(by=['air_year', 'air_week', 'program_start_time'])

# date_correction
data['start_time_cleaned'] = data['program_start_time'].apply(Clean_Fx.start_time)
data['end_time_cleaned'] = data['program_end_time'].apply(Clean_Fx.end_time)

# rearranging the data
data = data[['market', 'Station_Name', 'daypart_name', 'invcode_name',
             'invcode_External_Id', 'break_code', 'full_date', 'order_number',
             'order_created', 'program_start_time', 'start_time_cleaned',
             'program_end_time', 'end_time_cleaned', 'potential_units',
             'spot_counts', 'spot_rating_key', 'affiliation', 'gross_revenue',
             'air_year', 'air_week', 'order_year', 'Order_week',
             'full_datetime', 'created_datetime', 'last_updated']]
data = data.rename(str.lower, axis='columns')


# Grouping for TS Data

ts_groups = data.groupby(['market', 'station_name', 'daypart_name',
                          'invcode_name', 'air_week'])['air_year'].unique()
ts_groups = ts_groups.apply(list)
ts_groups = ts_groups.to_frame().reset_index()

# Merging the data frame with the groupby object
ts_data = pd.merge(ts_groups, data,  how='left',
                   on=['market', 'station_name', 'daypart_name',
                       'invcode_name', 'air_week'])

# Determining the amount of historical data for time series
# Current_Year = datetime.datetime.now().year
# TS_Years = list(pd.Series(range((Current_Year-4), (1+Current_Year))))

ts_data['time_series_flag'] = ts_data['air_year_x'].apply(Clean_Fx.TS_Check)

# view2 = data.head(1000)
# view = Median_data.head(1000)
# Restructuring the dataframe

data = ts_data.drop(columns=['air_year_x'])
data = data.rename(index=str, columns={'air_year_y': 'air_year'})
data = data.rename(str.lower, axis='columns')

del ts_data, ts_groups, current_year, ts_years

# Regrouping data to calculate the cumulative values

grouped_sum = data.groupby(['market', 'station_name',
                            'daypart_name', 'invcode_name',
                            'air_week', 'air_year'])['spot_counts'].sum()
grouped_sum = grouped_sum.reset_index()
grouped_sum = grouped_sum.rename(index=str,
                                 columns={'spot_counts': 'spot_sum'})


grouped_median = grouped_sum.groupby(['market', 'station_name',
                                      'daypart_name', 'invcode_name',
                                      'air_week'])['spot_sum'].median()
grouped_median = grouped_median.reset_index()
grouped_median = grouped_median.rename(index=str,
                                       columns={'spot_sum': 'spot_median'})

grouped_mad = grouped_sum.groupby(['market', 'station_name',
                                   'daypart_name', 'invcode_name',
                                   'air_week'])['spot_sum'].mad()
grouped_mad = grouped_mad.reset_index()
grouped_mad = grouped_mad.rename(index=str,
                                 columns={'spot_sum': 'spot_mad'})

grouped_median_mad = pd.merge(grouped_median, grouped_mad, how='left',
                              on=['market', 'station_name', 'daypart_name',
                                  'invcode_name', 'air_week'])
grouped_data = pd.merge(grouped_median_mad, grouped_sum, how='left',
                        on=['market', 'station_name', 'daypart_name',
                            'invcode_name', 'air_week'])

# Creating Upper and lower bound or spot_count
# Upper Bound  = Median +2*(Median Absolute Deviation)
# Lower_Bound = Median - 2*(Median Absolute Deviation)

# The value of n for lb and ub only needs to be changes here

grouped_data['spot_ub'] = (grouped_data['spot_median'] +
                           (3*grouped_data['spot_mad']))
grouped_data['spot_lb'] = (grouped_data['spot_median'] -
                           (3*grouped_data['spot_mad']))


grouped_data['spot_flag_temp'] = grouped_data.apply(Clean_Fx.Spot_Check,
                                                    axis=1)

data = pd.merge(grouped_data, data, how='left',
                on=['market', 'station_name', 'daypart_name',
                    'invcode_name', 'air_week', 'air_year'])

del grouped_mad, grouped_median, grouped_sum, grouped_median_mad, grouped_data

# Flagging Olympics and Sports
data['sport_olympic_flag'] = data.apply(Clean_Fx.SP_Olympic_Check, axis=1)

data['spot_flag'] = data.apply(Clean_Fx.future_check, axis=1)

final_table = data[['market', 'station_name', 'daypart_name', 'invcode_name',
                    'air_week', 'air_year', 'invcode_external_id',
                    'break_code', 'full_date', 'order_number', 'order_created',
                    'program_start_time', 'start_time_cleaned',
                    'program_end_time', 'end_time_cleaned', 'potential_units',
                    'spot_counts', 'spot_rating_key', 'affiliation',
                    'gross_revenue', 'order_year', 'order_week',
                    'full_datetime', 'created_datetime', 'last_updated',
                    'time_series_flag', 'spot_flag', 'sport_olympic_flag']]

normal_pile = final_table[final_table['sport_olympic_flag'] == 'normal']
sport_pile = final_table[final_table['sport_olympic_flag'] == 'sports']
olympic_pile = final_table[final_table['sport_olympic_flag'] == 'olympic']

# Good Pile

Fit_TS = normal_pile[(normal_pile['time_series_flag'] == 'fit')]
Good_Data = Fit_TS[Fit_TS['spot_flag'] == 'good']

# Bad_Pile

Unfit_TS = normal_pile[(normal_pile['time_series_flag'] == 'unfit')]
Fit_Bad_Spot = Fit_TS[Fit_TS['spot_flag'] == 'bad']

Bad_Data = Unfit_TS.append(Fit_Bad_Spot)

good_list = list((Good_Data.invcode_name).unique())
bad_list = list((Bad_Data.invcode_name).unique())

# Validation Script


def check_common(A, B):
    return list(set(B) & set(A))


pricegrid_inv = pd.read_sql(VALIDATION_QUERY, WIDE_ORBIT_CONNECT)
PriceGrid = set(pricegrid_inv['Inventory_code'].tolist())

check_common(pricegrid_inv, good_list)
good_list = set(good_list)

set(PriceGrid) & set(good_list)

common = set(list(PriceGrid.intersection(good_list)))

good_list - common

good_list


