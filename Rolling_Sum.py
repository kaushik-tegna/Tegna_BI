# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 12:47:53 2018

@author: SRanganath
"""
# from Cleaning_V2 import *
import numpy as np
import matplotlib.pyplot as plt

# Rolling_Sum.py

good_inv_list = list(Good_Data.invcode_name.unique())
bad_inv_list = list(Bad_Data.invcode_name.unique())

good_check = Good_Data[Good_Data.invcode_name == 'Today Show 7-8a']

group = good_check.groupby(['market',  'station_name', 'daypart_name',
                            'invcode_name', 'air_week' ,
                            'air_year'])['spot_counts']
group = group.apply(list)
group = group.reset_index()


def rolling_sum(X):
    arr = np.array(X)
    result = np.cumsum(arr)
    return result


group['rolling_sum'] = group.spot_counts.apply(rolling_sum)

group = good_check.groupby(['market',  'station_name', 'daypart_name',
                            'invcode_name', 'air_week' ,
                            'air_year'])

def cum_sum(Y):
    Y['cum_sum'] = Y['spot_counts'].cumsum()
    Y.reset_index(inplace=True)
    return Y

# Z=group.apply(cum_sum)



group = good_check.groupby(['market',  'station_name', 'daypart_name',
                            'invcode_name', 'air_week' ,
                            'air_year'])

bad_check = Bad_Data[Bad_Data.invcode_name == 'Today Show 7-8a']

plt.line('')





normal_pile = data[data['sport_olympic_flag'] == 'normal']
sport_pile = data[data['sport_olympic_flag'] == 'sports']
olympic_pile = data[data['sport_olympic_flag'] == 'olympic']

# Good Pile

Fit_TS = normal_pile[(normal_pile['time_series_flag'] == 'fit')]
Good_Data = Fit_TS[Fit_TS['spot_flag'] == 'good']

# Bad_Pile

Unfit_TS = normal_pile[(normal_pile['time_series_flag'] == 'unfit')]
Fit_Bad_Spot = Fit_TS[Fit_TS['spot_flag'] == 'bad']
Bad_Data = Unfit_TS.append(Fit_Bad_Spot)


test = Bad_Data[Bad_Data['spot_flag'] == 'good']