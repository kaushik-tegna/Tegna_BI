# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 16:18:48 2018

@author: SRanganath
"""
import re

# Function to check wether rows are fit for Time Series


def TS_Check(x):
    global ts_years
    if set(x) & set(ts_years) == set(ts_years):
        result = 'fit'
    else:
        result = 'unfit'
    return result


# Flagging based on spot counts


def Spot_Check(row):
    if (row['spot_sum'] >= row['spot_lb']
            and row['spot_sum'] <= row['spot_ub']):
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
