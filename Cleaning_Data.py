import pandas as pd
import pyodbc
import datetime
import re
'''Connection String and Query to fetch  data'''

conn = pyodbc.connect("""Driver={SQL Server Native Client 11.0};
                      Server=172.21.128.193; "
                      "Database=Wide_Orbit; UID=etl_user_prod; 
                      pwd=T@gna2018;""")
query = '''SELECT * FROM [Wide_Orbit].[dbo].[buyer_demand_fcst_input_new2]
         where [Market] = \'Atlanta\''''

df = pd.read_sql(query, conn)

view_df = df.head(10000)

'''Exploratory Analysis'''

df_null = df.isnull().sum()

df_null = df_null.to_frame()

desc_df = df.describe()

df_null('program_start_time')

view_df = view_df.sort_values(by = ['air_year','air_week',
                                    'program_start_time'])

view_df['program_start_time']

view_df['program_end_time']

dtype=df.dtypes

'''date_correction'''

view_df['prg_strt_time'] = [datetime.datetime.time(d) for 
       d in view_df['program_start_time']]
view_df['prg_end_time'] = [datetime.datetime.time(d) for 
       d in view_df['program_end_time']]

'''rearranging the cleaned columns'''
list(view_df)

view_df = view_df[
        ['market', 'Station_Name','daypart_name','invcode_name',
         'invcode_External_Id','break_code','full_date','order_number',
         'order_created','program_start_time','prg_strt_time',
         'program_end_time','prg_end_time','potential_units','spot_counts',
         'spot_rating_key','affiliation','gross_revenue','air_year','air_week',
         'order_year','Order_week','full_datetime','created_datetime', 
         'last_updated']]

'''Treating null values, removing SP dayparts with olympics'''

null_df = df.query('program_start_time == "NaT"')

sp_df = df[(df.daypart_name == 'SP')]

p = re.compile('.*(olympic).*', re.IGNORECASE)
olympics_df = null_df[(null_df.daypart_name == 'SP') & 
                      null_df.invcode_name.str.match(p)]
                      

other_df = null_df[(null_df.daypart_name != 'SP')]

# & (df.invcode_name  == 'Olympic')

null_df.daypart_name.unique()

from pandas.util.testing import assert_frame_equal
assert_frame_equal(null_df, olympics_df)

def check_frame():
    try:
        assert_frame_equal(null_df, olympics_df)
        return True
    except:  # appeantly AssertionError doesn't catch all
        return False
check_frame()

temp_df = df[(df.daypart_name =='SP')]

temp_df = df[(df.daypart_name == 'SP') & (df.invcode_name.str.match('.(rio).*'))]

grp_df=df.groupby(['daypart_name','invcode_name','air_year','air_week'])