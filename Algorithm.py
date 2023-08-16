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

            # Initialize cache_trans (dataframe store productid to transfer - max transfer to min receive)
            cache_trans = pool_2_clone[pool_2_clone['productId'] == pool_1_clone.iloc[0]['productId']]

            ## Rule: Compare the productId and Balance number
            # Initialize transactions dataframe to store logs of internal transactions
            transactions = pd.DataFrame(columns = ['storeId_transfer','storeName_transfer', 'storeId_receive'
                                                    ,'storeName_receive','productId','productName','Quantity'])

            # Initialize pool_3 dataframe to store residual productId and no need to receive => Move to HO
            pool_3 = pd.DataFrame(columns = ['storeId_HO','storeName_HO', 'productId','productName','Quantity'])

            # start while loop to distribute goods
            while True:
                # Initialize amount of residual goods in pool_1:
                n = pool_1_clone.iloc[0]['Balance_num']

                # resolve issue when cache_trans is blank (no need to receive => move goods to HO), create instance dict to store residual goods and concat to pool_3
                if len(cache_trans.index) == 0  :
                    residual_dict = {'storeId_HO': pool_1_clone.iloc[0]['storeId'],
                                    'storeName_HO': pool_1_clone.iloc[0]['storeName'],
                                    'productId': pool_1_clone.iloc[0]['productId'],
                                    'productName': pool_1_clone.iloc[0]['productName'],
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
                    translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_trans.iloc[0]['storeId'],
                                     'storeName_receive':  cache_trans.iloc[0]['storeName'],
                                     'productId':          pool_1_clone.iloc[0]['productId'],
                                     'productName':        pool_1_clone.iloc[0]['productName'],'Quantity': m}
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
                    translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_trans.iloc[0]['storeId'],
                                     'storeName_receive':  cache_trans.iloc[0]['storeName'],
                                     'productId':          pool_1_clone.iloc[0]['productId'],
                                     'productName':        pool_1_clone.iloc[0]['productName'], 'Quantity': n}
                    # Add logs to transactions dataframe
                    transactions = pd.concat([transactions, pd.DataFrame(data= translog_dict, index=[0])], ignore_index= False)

                    # update balance_num in pool_2_clone
                    pool_2_clone.loc[cache_trans.index[0],['Balance_num']] = remain_amount

                    # update stock in pool_2_clone (to run Round 2: HO to Areas) ***
                    pool_2_clone.loc[cache_trans.index[0],['StockQuantity']] = pool_2_clone.loc[cache_trans.index[0],['StockQuantity']] + n

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

            # Append residual goods to HO to pool_1_to_HO dataframe
            pool_1_to_HO = pd.concat([pool_1_to_HO, pool_3], ignore_index= True)

            # Append deficient goods still remain in pool_2_clon
            pool_2_remain = pd.concat([pool_2_remain, drop_sub_columns_pool2(pool_2_clone)], ignore_index= True)

        else: # Area != HCM & HN
            # Initialize pool_1 & pool_2 clone
            pool_1_clone = pool_1
            pool_2_clone = pool_2

            # Append residual goods to dataframe pool_1_to_HO from pool_clone -> Goods is still residual
            pool_1_clone = drop_sub_columns_pool1(pool_1_clone.rename(columns={'Balance_num':'Quantity'
                                                                , 'storeId':'storeId_HO', 'storeName':'storeName_HO'}))
            pool_1_to_HO = pd.concat([pool_1_to_HO, pool_1_clone], ignore_index= True)

            # Appen deficient goods to pool_2_remain -> Goods is still deficient
            pool_2_remain = pd.concat([pool_2_remain, drop_sub_columns_pool2(pool_2_clone)], ignore_index= True)

            ## FINISH Round 1

    # Export excel files to write goods is still residual or deficient after first round of distribution
    pool_1_to_HO.to_excel(f'{path}Hàng dư - Move HO (Round 1).xlsx', encoding = 'utf-8-sig', index = False)
    pool_2_remain.to_excel(f'{path}Hàng thiếu - Sau (Round 1).xlsx', encoding = 'utf-8-sig', index = False)

    # Rename cols in pool_1_to_HO again - to re-use in Round 2
    pool_1_to_HO.rename(columns={'storeId_HO':'storeId', 'storeName_HO':'storeName', 'Quantity':'StockQuantity'})

    ### ROUND 2: After HO receive residual Stocks from Store, then HO transfer goods to Stores that is still deficient

    # First, initialize dataframe of Stocks in HO (after receive residual goods)
    HO_Stocks = pd.concat([df_1, pool_1_to_HO], ignore_index= True)

    # Processing HO_Stocks dataframe
    HO_Stocks = HO_Stocks.groupby(by=['productId', 'productName'])['StockQuantity'].sum()
    HO_Stocks = HO_Stocks.reset_index()
    HO_Stocks['storeId'] = '88003'
    HO_Stocks['storeName'] = 'KHO_DP2'
    HO_Stocks['Area'] = 'KHO_DP2'
    HO_Stocks['SO1'] = 0
    HO_Stocks['SO2'] = 0
    HO_Stocks['SO3'] = 0
    HO_Stocks['SO4'] = 0
    HO_Stocks['AvgSO'] = 0
    HO_Stocks = HO_Stocks.reindex(columns=['Area', 'storeId', 'storeName', 'productId', 'productName', 'SO1', 'SO2', 'SO3', 'SO4', 'AvgSO', 'StockQuantity'])

    # Write an excel file - HO_Stocks after receive residual goods (if needed)
    # HO_Stocks.to_excel(f"{path}HO sau nhận hàng Dư.xlsx", encoding = 'utf-8-sig', index= False)

    # Initialize data_round2 - df store data that Combine HO_Stocks and deficients goods
    R2_data = pd.concat([HO_Stocks, pool_2_remain], ignore_index= True)

    # processing data_round2
    R2_data = R2_data.fillna(np.nan).replace([np.nan], 0)
    R2_data['AvgSO'] = R2_data['AvgSO'].clip(lower=0)
    R2_data['SellPower'] = round(R2_data['AvgSO'].div(7),3)
    R2_data['DOS'] = round(R2_data['StockQuantity']/ data_round2['SellPower'],0)
    R2_data = R2_data.sort_values(by = ['DOS'], ascending= False)
    R2_data['Statement'] = R2_data['DOS'].apply(lambda x: DOS_Classify(x))
    R2_data['Balance_num'] = R2_data.apply(lambda row: cal_balance_num(row['Statement'], row['StockQuantity'], row['SellPower']), axis = 1)

    # write excel file to store data of data_round2 (if needed)
    # data_round2.excel(f'{path}Data (Round 2).xlsx', encoding = 'utf-8-sig', index = False)

    # Initialize pool in round 2
    R2_pool_1 = sort_by_balance(R2_data[R2_data['Statement'] == "residual"])
    R2_pool_2 = sort_by_balance(R2_data[R2_data['Statement'] == "deficient"])

    # Initialize clone pool
    R2_pool_1_clone = R2_pool_1
    R2_pool_2_clone = R2_pool_2

    # Initialize cache_trans round 2
    R2_cache_trans = R2_pool_2_clone[R2_pool_2_clone['productId'] == R2_pool_1_clone.iloc[0]['productId']]

    # Initialize R2_trans - dataframe to store logs of distribute transactions
    R2_transactions = pd.DataFrame(columns = ['storeId_transfer','storeName_transfer', 'storeId_receive'
                                    ,'storeName_receive','productId','productName','Quantity'])

    ## Don't need to create pool_3 - residual goods to HO
    while True:
        # Amount of residual goods
        n = R2_pool_1_clone.iloc[0]['Balance_num']

        # Resolve issue when cache_trans is blank => Don't have deficient goods
        if len(R2_cache_trans.index) == 0:
            # Don't need to write logs of transaction to HO
            # Drop residual goods in pool_1 but not need to transfer in pool_2
            R2_pool_1_clone.drop(index = R2_pool_1_clone.loc[0:1,:].index, inplace = True)

            # BREAK when Pool_1 is over => Don't have residual goods to transfer
            if len(R2_pool_1_clone.index) == 0:
                break

            # update cache_trans & continue to outer loop
            R2_cache_trans = R2_pool_2_clone[R2_pool_2_clone['productId'] == R2_pool_1_clone.iloc[0]['productId']]

            continue

        # Amount of deficient goods
        m = R2_cache_trans.iloc[0]['Balance_num']

        ## Case 1: n >= m (transfer quantity > receive quantity)
        if n >= m:
            remain_amount = n - m

            # Initialize dict to store internal transactions
            translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                             'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                             'storeId_receive':    cache_trans.iloc[0]['storeId'],
                             'storeName_receive':  cache_trans.iloc[0]['storeName'],
                             'productId':          pool_1_clone.iloc[0]['productId'],
                             'productName':        pool_1_clone.iloc[0]['productName'],'Quantity': m}

            # Concat dict to transaction df
            R2_transactions = pd.concat([R2_transactions, pd.DataFrame(data = translog_dict, index=[0])], ignore_index= False)

            # Update value in 2 pools
            R2_pool_1_clone.loc[R2_pool_1_clone.index[0], ['Balance_num']] = remain_amount
            R2_pool_2_clone.drop(index = R2_cache_trans.iloc[0:1,:].index, inplace = True)

            # Re-Sorting 2 pools
            R2_pool_1_clone = sort_by_balance(R2_pool_1_clone)
            R2_pool_2_clone = sort_by_balance(R2_pool_2_clone)

            # BREAK when pool_2 is over => No need to receive goods
            if len(R2_pool_2_clone.index) == 0:
                break

            # Update cache_trans for next loop
            R2_cache_trans = R2_pool_2_clone[R2_pool_2_clone['productId'] == R2_pool_1_clone.iloc[0]['productId']]

            continue

        ## Case 2: m > n (receive quantity > transfer quantity)
        elif m > n:
            remain_amount = m - n

            # Initialize dict to store internal transactions
            translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                             'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                             'storeId_receive':    cache_trans.iloc[0]['storeId'],
                             'storeName_receive':  cache_trans.iloc[0]['storeName'],
                             'productId':          pool_1_clone.iloc[0]['productId'],
                             'productName':        pool_1_clone.iloc[0]['productName'],'Quantity': n}

            # Concat dict to transaction df
            R2_transactions = pd.concat([R2_transactions, pd.DataFrame(data = translog_dict, index= [0])], ignore_index= False)

            # Update 2 pools
            R2_pool_2_clone.loc[R2_cache_trans.index[0], ['Balance_num']] = remain_amount
            R2_pool_1_clone.drop(index = R2_pool_1_clone.iloc[0:1, :].index, inplace = True)

            # Re-Sorting 2 pools
            R2_pool_1_clone = sort_by_balance(R2_pool_1_clone)
            R2_pool_2_clone = sort_by_balance(R2_pool_2_clone)

            # BREAK when pool_1 is over => No goods to transfer
            if len(R2_pool_1_clone.index) == 0:
                break

            # Update cache_trans for next loop
            R2_cache_trans = R2_pool_2_clone[R2_pool_2_clone['productId'] == R2_pool_1_clone.iloc[0]['productId']]

            continue

        else:
            None


        # filter transaction quantity > 0
        R2_transactions = R2_transactions[R2_transactions['Quantity'] > 0]





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
