# import libs
import pandas as pd
import numpy as np
import math
import re
import warnings

warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
pd.set_option('mode.chained_assignment', 'raise')

# define file path
path = "/Users/hoangpham/Downloads/GoodsDistribution/"
path_ho = "/Users/hoangpham/Downloads/GoodsDistribution/HO_Stock.xlsx"

# define dataframe
df = pd.read_excel(f'{path}Stocks.xlsx')
list_area = df['Area'].values.tolist()

new_area = [re.sub(r'\d+', "", re.sub(r'\s+', "", area)) for area in list_area]
df = df.drop(['Area'], axis =1)
df['Area'] = new_area

list_area_2 = df['Area'].unique().tolist()

# define dataframe from GGS to drop productId
sheet_id = "1L4hS0L_4VS0D3EM283oDf1d8KizIdV09ENc71qgLYQY"
worksheet = "1408717402"
ggs_url = "https://docs.google.com/spreadsheets/d/{0}/export?gid={1}&format=csv".format(sheet_id, worksheet)
drop_df = pd.read_csv(ggs_url)
id_drop = drop_df['ProductId'].astype(str).unique().tolist()

# drop rows in df with productId in drop_df
df = df[~df['productId'].isin(id_drop)]

# define another dataframe
# dataframe store Stocks in HO - KHO DP2
df_1 = pd.read_excel(path_ho)

# define min & max DOS (optimize 30 days)
min_day = 26
max_day = 34

# define drop columns, sort orders, classification good's statement function
# drop cols in pool_1
def drop_sub_columns_pool1(df):
    df = df.drop(['StockQuantity', 'SO1', 'SO2', 'SO3', 'SO4', 'AvgSO', 'SellPower', 'DOS', 'Statement'],axis=1)
    return df

# Drop cols in pool_2
def drop_sub_columns_pool2(df):
    df = df.drop(['SellPower', 'DOS', 'Statement', 'Balance_num'],axis=1)
    return df

# Sort by Balance_num
def sort_by_balance(df):
    df = df.sort_values(by='Balance_num' ,ascending=False)
    return df

# DOS Classify
def DOS_Classify(dos):
    if dos < min_day:
        return "deficient" # Thiếu
    elif min_day <= dos <= max_day:
        return "sufficient" # Đủ
    else :
        return "residual" # Dư

# Calculate balance quantity
def cal_balance_num(state, stock, power):
    if state == "residual":
        return math.ceil(stock - power * max_day)
    elif state == "deficient":
        return round(-stock + power * min_day,0)
    else:
        return 0

# Function to allot goods
def allot(df_1, df_2):
    if df_2['Area'] == "HồChíMinh" or df_2['Area'] == "HàNội":
        pool_1_transfer = sort_by_balance(df_2[df_2['Statement']=='residual'])
        pool_2_get = sort_by_balance(df_2[df_2['Statement']=='deficient'])
        pritn(pool_1_transfer)


# define df_2 copy of df (main stocks's df)
df_2 = df[['Area','storeId','storeName', 'productId','productName'
            ,'SO1','SO2','SO3','SO4', 'AvgSO','StockQuantity']].copy()
df_2 = df_2.fillna(0)
# nomarlize values of AvgSO, If < 0 => 0
df_2['AvgSO'] = df_2['AvgSO'].clip(lower=0)
# calculate SellPower per 1 day
df_2['SellPower'] = round(df_2['AvgSO'].div(7),3)
# calculate Day of Sales
df_2['DOS'] = round(df_2['StockQuantity']/df_2['SellPower'],0)
# sorted df_2 by DOS desc
df_2 = df_2.sort_values(by=['DOS'], ascending= False)
# calculate DOS statement
df_2['Statement'] = df_2['DOS'].apply(lambda x: DOS_Classify(x))
# calculate balance quantity
df_2['Balance_num'] = df_2.apply(lambda row: cal_balance_num(row['Statement'], row['StockQuantity'], row['SellPower']), axis = 1)
df_2 = [df_2[df_2.Area == list_area_2[area]] for area in range(0,len(list_area_2))]

# define pool_1 (store residual items); pool_2 (store deficient items)
#pool_1 = sort_by_balance(df_2[df_2['Statement']=='residual'])
#pool_2 = sort_by_balance(df_2[df_2['Statement']=='deficient'])




