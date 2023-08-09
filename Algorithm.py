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
    for df in df_2:
        # create copy dataframe
        data = df.copy()
        # Initialize pool_1(transfer), pool_2(recevice)
        pool_1 = sort_by_balance(data[data['Statement']=='residual'])
        pool_2 = sort_by_balance(data[data['Statement']=='deficient'])
        pool_1_to_HO = pd.DataFrame()
        pool_2_remain = pd.DataFrame()

        if list(data.iterrows())[0][1]['Area'] == "HồChíMinh" or list(data.iterrows())[0][1]['Area'] == "HàNội":
            pool_1_clone = pool_1
            pool_2_clone = pool_2

            # Initialize cache_transaction (dataframe store productid to transfer - max transfer to min receive)
            cache_trans = pool_2_clone[pool_2_clone['productId'] == pool_1_clone.iloc[0]['productId']]

            ## Rule: Compare the productId and Balance number
            # Initialize transactions dataframe to store logs of internal transactions
            transactions = pd.DataFrame(colums = ['storeId_transfer','storeName_transfer', 'storeId_receive'
                                                    ,'storeName_receive','productId','productName','Quantity'])

            # Initialize pool_3 dataframe to store residual productId and no need to receive => Move to HO
            pool_3 = pd.DataFrame(columns = ['storeId_HO','storeName_HO', 'productId','productName','Quantity'])

            # start while loop to distribute goods
            while True:
                # Initialize amount of residual goods in pool_1:
                n = pool_1_clone.iloc[0]['Balance_num']

                # resolve issue when cache_transaction is blank (no need to receive => move goods to HO), create instance dict to store residual goods and concat to pool_3
                if len(cache_trans.index) == 0  :
                    residual_dict = {'storeId_HO': pool_1_transfer_clone.iloc[0]['storeId'],
                                    'storeName_HO': pool_1_transfer_clone.iloc[0]['storeName'],
                                    'productId': pool_1_transfer_clone.iloc[0]['productId'],
                                    'productName': pool_1_transfer_clone.iloc[0]['productName'],
                                    'Quantity': n}

                    pool_3 = pd.concat([pool_3, pd.DataFrame(data = residual_dict, index=[0])], ignore_index= False)

                    # drop residual goods in pool_1_clone:
                    pool_1_clone.drop(index= pool_1_clone.iloc[0:1,:].index, inplace = True)

                    # break loop if len(pool_1_clone) = 0 - when residual goods is over
                    if len(pool_1_clone.index) == 0:
                        break

                    # update cache_trans with new productId at first index of pool_1_clone
                    cache_trans = pool_2_clone[pool_2_clone['productId'] == pool_1_clone.iloc[0]['productId']]
                    # continue loop outer if
                    continue

                # Initialize amount of deficient goods in cache_trans:
                m = cache_trans.iloc[0]['Balance_num']

                ## Case 1: n > m; transfer > receive
                if n > m:
                    # Initialize amount of residual goods remain
                    remain_amount = n - m

                    # Ininitalize dict to write log of transaction
                    translog_dict = {'storeId_transfer': pool_1_transfer_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_transfer_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_transaction.iloc[0]['storeId'],
                                     'storeName_receive':  cache_transaction.iloc[0]['storeName'],
                                     'productId':          pool_1_transfer_clone.iloc[0]['productId'],
                                     'productName':        pool_1_transfer_clone.iloc[0]['productName'],'Quantity': m}
                    # concat to transactions dataframe
                    transactions = pd.concat([transactions, pd.DataFrame(data = translog_dict, index= [0])], ignore_index= False)

                    # update balance_num in 2 pools
                    # update pool_1_clone
                    pool_1_clone.loc[pool_1_clone.index[0], ['Balance_num']] = remain_amount
                    # update pool_2_clone
                    pool_2_clone.drop(index= cache_trans.iloc[0:1,:].index, inplace= True)

                    # re-sorting pool to find out first index goods
                    pool_1_clone = sort_by_balance(pool_1_clone)
                    pool_2_clone = sort_by_balance(pool_2_clone)

                    # BREAK: When pool_2 - deficient goods is over, no need to receive more
                    if len(pool_2_clone.index) == 0:
                        break

                    # If loop is exist, then update cache_trans
                    cache_trans = pool_2_clone[pool_2_clone['productId'] == pool_1_clone.iloc[0]['productId']]

                    # Continue to the next loop
                    continue

                ## Case 2: m > n and n != 0 => receive > transfer
                elif m > n and (n != 0 or n != None):
                    remain_amount = m - n

                    # Intialize dict to store log of transaction
                    translog_dict = {'storeId_transfer': pool_1_transfer_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_transfer_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_transaction.iloc[0]['storeId'],
                                     'storeName_receive':  cache_transaction.iloc[0]['storeName'],
                                     'productId':          pool_1_transfer_clone.iloc[0]['productId'],
                                     'productName':        pool_1_transfer_clone.iloc[0]['productName'], 'Quantity': n}
                    # Add logs to transactions dataframe
                    transactions = pd.concat([transactions, pd.DataFrame(data= translog_dict, index=[0])], ignore_index= False)

                    # update balance_num in pool_2_clone
                    pool_2_clone.loc[cache_trans.iloc[0].index,['Balance_num']] = remain_amount

                    # update stock in pool_2_clone (to run Round 2: HO to Areas) ***
                    pool_2_clone.loc[cache_trans.iloc[0].index,['StockQuantity']] = pool_2_clone.loc[cache_trans.iloc[0].index,['StockQuantity']] + n

                    # drop goods is over in pool_1
                    pool_1_clone.drop(index = pool_1_clone.iloc[0:1, :].index, inplace = True)

                    # Re-sorting pool
                    pool_1_clone = sort_by_balance(pool_1_clone)
                    pool_2_clone = sort_by_balance(pool_2_clone)

                    # BREAK when goods in pool_1_clone is over
                    if len(pool_1_clone.index)==0:
                        break

                    # Update cache_trans and continue loop
                    cache_trans = pool_2_clone[pool_2_clone['productId']==pool_1_clone.iloc[0]['productId']]
                    continue

                ## Case 3: Receive > Transfer (m > n) (1 - 0 = 1, case when n = 0 or n = None)
                elif m > n and (n == 0 or n == None):
                    # remain_amount = 0
                    # m does not have chane => don't need to update pool_2_clone

                    # drop n in pool_1_clone
                    pool_1_clone.drop(index=pool_1_clone.iloc[0:1,:].index ,inplace=True)

                    # Re-sorting pool
                    pool_1_clone = sort_by_balance(pool_1_clone)
                    pool_2_clone = sort_by_balance(pool_2_clone)

                    # BREAK when pool_1_clone is over
                    if len(pool_1_clone.index) == 0:
                        break

                    # Update cache_trans and continue loop
                    cache_trans = pool_2_clone[pool_2_clone['productId'] == pool_1_clone.iloc[0]['productId']]
                    continue

                ## Case 4: Nothing
                else: None

            # Filter transactions DataFrame:
            transactions = transactions[transactions['Quantity'] > 0]

            # Filter goods to HO with quantity > 0:
            pool_3 = pool_3[pool_3['Quantity']>0]

            # define excel filepath
            out_path = f"{path}{data.iloc[0]['Area']}.xlsx" # split Areas HCM & HN
            writer = pd.ExcelWriter(out_path , engine = 'xlsxwriter')

            # export excel file store transactions
            transactions.to_excel(writer, sheet_name=f"trans_{data.iloc[0]['Area']}", encoding= 'utf-8-sig', index= False)

            # export excel file residual goods to HO
            pool_3.to_excel(writer, sheet_name=f"{data.iloc[0]['Area']}_transHO", encoding="utf-8-sig", index= False)
            writer.save()

            #




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


allot(df_1, df_2)


