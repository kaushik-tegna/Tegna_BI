# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 16:18:48 2018

@author: SRanganath
"""
import re
import pandas as pd
import datetime

# Time calculations
# current_year = datetime.datetime.now().year
# ts_years = list(pd.Series(range((current_year-3), (1+current_year))))
# Function to check wether rows are fit for Time Series


def start_time(X):
    result = datetime.datetime.time(X)
    return result


def end_time(X):
    result = datetime.datetime.time(X)
    return result


def TS_Check(x):
    current_year = datetime.datetime.now().year
    ts_years = list(pd.Series(range((current_year-3), (1+current_year))))
    if set(x) & set(ts_years) == set(ts_years):
        result = 'fit'
    else:
        result = 'unfit'
    return result


# Flagging based on spot counts


def Spot_Check(row):
    if((row['spot_sum'] >= row['spot_lb']) and
       (row['spot_sum'] <= row['spot_ub'])):
        result = 'good'
    else:
        result = 'bad'
    return result


# Flagging Olympics and sports

olympic_pattern = re.compile('.*(olympic).*', re.IGNORECASE)


def SP_Olympic_Check(row):
    global olympic_pattern
    if (row['daypart_name'] == 'SP'):
        m = re.match(olympic_pattern, row['invcode_name'])
        if m:
            result = 'olympic'
        else:
            result = 'sports'
    else:
        result = 'normal'
    return result


def future_check(row):
    current_date = datetime.datetime.now()
    current_year = datetime.datetime.now().year
    current_week = int(current_date.strftime("%V"))
    if(int(row['air_year']) > int(current_year)):
        result = 'good'
    elif((int(row['air_year'] == int(current_year)))and
         int(row['air_week']) >= int(current_week)):
        result = 'good'
    else:
        result = row['spot_flag_temp']
    return result

# Checking orders in the future

# def future_check(row):
#    current_date = datetime.datetime.now()
#    current_year = datetime.datetime.now().year
#    current_week = int(current_date.strftime("%V"))
#    if(row['air_year'] >= current_year
#       and row['air_week'] >= current_week):
#        result = 'good'
#    else:
#        result = row['spot_flag']
#        return result
#
# good_check = Good_Data[Good_Data.invcode_name == 'Today Show 7-8a']
# group = good_check.groupby(['market',  'station_name', 'daypart_name',
#                            'invcode_name', 'air_week',
#                            'air_year'])
#
#
# def cum_sum(Y):
#     Y['cum_sum'] = Y['spot_counts'].cumsum()
#     Z = Y.reset_index()
#     return Z


# def test_code(row):
#     if(row['spot_flag'] == row['Spot_new']):
#         result = 'same'
#     else:
#         result = 'check'
#     return result
#
#
# data1['test'] = data1.apply(test_code, axis=1)
#
# check = data1[data1['test'] == 'check']


# def rolling_sum(X):
#     arr = np.array(X)
#     result = np.cumsum(arr)
#     return result
#
#
# group['rolling_sum'] = group.spot_counts.apply(rolling_sum)
#
# group = good_check.groupby(['market',  'station_name', 'daypart_name',
#                             'invcode_name', 'air_week' ,
#                             'air_year'])
#
# def cum_sum(Y):
#     Y['cum_sum'] = Y['spot_counts'].cumsum()
#     Y.reset_index(inplace=True)
#     return Y
#
#   Z=group.apply(cum_sum)
