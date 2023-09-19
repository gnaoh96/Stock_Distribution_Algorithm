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

# define dataframe from GGS to drop productId
sheet_id = "1L4hS0L_4VS0D3EM283oDf1d8KizIdV09ENc71qgLYQY"
worksheet = "1408717402"
ggs_url = "https://docs.google.com/spreadsheets/d/{0}/export?gid={1}&format=csv".format(sheet_id, worksheet)
drop_df = pd.read_csv(ggs_url)
id_drop = drop_df['ProductId'].astype(str).unique().tolist()

# define dataframe
df = pd.read_excel(f'{path}Stocks.xlsx')
df = df[~df['productId'].astype(str).isin(id_drop)]
df_0 = df

# define list
list_area = df['Area'].values.tolist()

new_area = [re.sub(r'\d+', "", re.sub(r'\s+', "", area)) for area in list_area]
df = df.drop(['Area'], axis =1)
df['Area'] = new_area

list_area_2 = df['Area'].unique().tolist()


# drop rows in df with productId in drop_df


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
def round1(df_2):
    for df in df_2:
        # create copy dataframe
        data = df.copy()
        # Initialize pool_1(transfer), pool_2(recevice)
        pool_1 = sort_by_balance(data[data['Statement']=='residual'])
        pool_2 = sort_by_balance(data[data['Statement']=='deficient'])
        pool_1_to_HO = pd.DataFrame()
        pool_2_remain = pd.DataFrame()
        province_to_HO = pd.DataFrame()
        province_remain = pd.DataFrame()

        # ROUND_1_HaNoi
        if list(data.iterrows())[0][1]['Area'] == "HàNội":
            hn_trans = pd.DataFrame()
            hn_to_HO = pd.DataFrame()
            hn_remain = pd.DataFrame()
            pool_1_clone = pool_1
            pool_2_clone = pool_2

            # Initialize cache_trans (dataframe store productid to transfer - max transfer to min receive)
            cache_trans = pool_2_clone[pool_2_clone['productId'] == pool_1_clone.iloc[0]['productId']]

            ## Rule: Compare the productId and Balance number
            # Initialize transactions dataframe to store logs of internal transactions
            # transactions = pd.DataFrame(columns = ['storeId_transfer','storeName_transfer', 'storeId_receive'
            #     ,'storeName_receive','productId','productName','Quantity'])

            # Initialize pool_3 dataframe to store residual productId and no need to receive => Move to HO
            pool_3 = pd.DataFrame(columns = ['Area' ,'storeId_HO','storeName_HO', 'Cat2022', 'productId','productName','Quantity'])

            # start while loop to distribute goods
            while True:
                # Initialize amount of residual goods in pool_1:
                n = pool_1_clone.iloc[0]['Balance_num']

                # resolve issue when cache_trans is blank (no need to receive => move goods to HO), create instance dict to store residual goods and concat to pool_3
                if len(cache_trans.index) == 0  :
                    residual_dict = {'Area': pool_1_clone.iloc[0]['Area'],
                                     'storeId_HO': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_HO': pool_1_clone.iloc[0]['storeName'],
                                     'Cat2022': pool_1_clone.iloc[0]['Cat2022'],
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
                if n >= m:
                    # Initialize amount of residual goods remain
                    remain_amount = n - m

                    # Ininitalize dict to write log of transaction
                    translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_trans.iloc[0]['storeId'],
                                     'storeName_receive':  cache_trans.iloc[0]['storeName'],
                                     'Cat2022' : pool_1_clone.iloc[0]['Cat2022'],
                                     'productId':          pool_1_clone.iloc[0]['productId'],
                                     'productName':        pool_1_clone.iloc[0]['productName'],'Quantity': m}
                    # concat to transactions dataframe
                    hn_trans = pd.concat([hn_trans, pd.DataFrame(data = translog_dict, index= [0])], ignore_index= False)

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
                    continue

                ## Case 2: m > n and n != 0 => receive > transfer
                elif m > n and (n != 0 or n != None):
                    remain_amount = m - n

                    # Intialize dict to store log of transaction
                    translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_trans.iloc[0]['storeId'],
                                     'storeName_receive':  cache_trans.iloc[0]['storeName'],
                                     'Cat2022' : pool_1_clone.iloc[0]['Cat2022'],
                                     'productId':          pool_1_clone.iloc[0]['productId'],
                                     'productName':        pool_1_clone.iloc[0]['productName'], 'Quantity': n}
                    # Add logs to transactions dataframe
                    hn_trans = pd.concat([hn_trans, pd.DataFrame(data= translog_dict, index=[0])], ignore_index= True)

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

                else:
                    break

            hn_trans = hn_trans[hn_trans['Quantity'] > 0] # file 1
            pool_3 = pool_3[pool_3['Quantity'] > 0]
            hn_to_HO = pd.concat([hn_to_HO, pool_3], ignore_index= True) # file 2
            hn_remain = pd.concat([hn_remain, drop_sub_columns_pool2(pool_2_clone)], ignore_index= True) # file 3

        # Round_1_HoChiMinh
        elif list(data.iterrows())[0][1]['Area'] == "HồChíMinh":
            hcm_trans = pd.DataFrame()
            hcm_to_HO = pd.DataFrame()
            hcm_remain = pd.DataFrame()
            pool_1_clone = pool_1
            pool_2_clone = pool_2

            # Initialize cache_trans (dataframe store productid to transfer - max transfer to min receive)
            cache_trans = pool_2_clone[pool_2_clone['productId'] == pool_1_clone.iloc[0]['productId']]

            ## Rule: Compare the productId and Balance number
            # Initialize transactions dataframe to store logs of internal transactions
            # transactions = pd.DataFrame(columns = ['storeId_transfer','storeName_transfer', 'storeId_receive'
            #     ,'storeName_receive','productId','productName','Quantity'])

            # Initialize pool_3 dataframe to store residual productId and no need to receive => Move to HO
            pool_3 = pd.DataFrame(columns = ['Area','storeId_HO','storeName_HO', 'Cat2022', 'productId','productName','Quantity'])

            # start while loop to distribute goods
            while True:
                # Initialize amount of residual goods in pool_1:
                n = pool_1_clone.iloc[0]['Balance_num']

                # resolve issue when cache_trans is blank (no need to receive => move goods to HO), create instance dict to store residual goods and concat to pool_3
                if len(cache_trans.index) == 0  :
                    residual_dict = {'Area' : pool_1_clone.iloc[0]['Area'],
                                     'storeId_HO': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_HO': pool_1_clone.iloc[0]['storeName'],
                                     'Cat2022': pool_1_clone.iloc[0]['Cat2022'],
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
                if n >= m:
                    # Initialize amount of residual goods remain
                    remain_amount = n - m

                    # Ininitalize dict to write log of transaction
                    translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_trans.iloc[0]['storeId'],
                                     'storeName_receive':  cache_trans.iloc[0]['storeName'],
                                     'Cat2022' : pool_1_clone.iloc[0]['Cat2022'],
                                     'productId':          pool_1_clone.iloc[0]['productId'],
                                     'productName':        pool_1_clone.iloc[0]['productName'],'Quantity': m}
                    # concat to transactions dataframe
                    hcm_trans = pd.concat([hcm_trans, pd.DataFrame(data = translog_dict, index= [0])], ignore_index= False)

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
                    continue

                ## Case 2: m > n and n != 0 => receive > transfer
                elif m > n and (n != 0 or n != None):
                    remain_amount = m - n

                    # Intialize dict to store log of transaction
                    translog_dict = {'storeId_transfer': pool_1_clone.iloc[0]['storeId'],
                                     'storeName_transfer': pool_1_clone.iloc[0]['storeName'],
                                     'storeId_receive':    cache_trans.iloc[0]['storeId'],
                                     'storeName_receive':  cache_trans.iloc[0]['storeName'],
                                     'Cat2022' : pool_1_clone.iloc[0]['Cat2022'],
                                     'productId':          pool_1_clone.iloc[0]['productId'],
                                     'productName':        pool_1_clone.iloc[0]['productName'], 'Quantity': n}
                    # Add logs to transactions dataframe
                    hcm_trans = pd.concat([hcm_trans, pd.DataFrame(data= translog_dict, index=[0])], ignore_index= True)

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

                else:
                    break

            # Processing data in HCM after Round_1_HCM
            hcm_trans = hcm_trans[hcm_trans['Quantity'] > 0] # file 1
            pool_3 = pool_3[pool_3['Quantity'] > 0]
            hcm_to_HO = pd.concat([hcm_to_HO, pool_3], ignore_index= True) # file 2
            hcm_remain = pd.concat([hcm_remain, drop_sub_columns_pool2(pool_2_clone)], ignore_index= True) # file 3

        # Round_1_other_Provinces
        elif list(data.iterrows())[0][1]['Area'] == "ĐàNẵng":
            # Initialize pool_1 & pool_2 clone
            pool_1_clone = pool_1
            pool_2_clone = pool_2
            dn_to_ho = pd.DataFrame()
            dn_remain= pd.DataFrame()

            # Append residual goods to dataframe pool_1_to_HO from pool_clone -> Goods is still residual
            pool_1_clone = drop_sub_columns_pool1(pool_1_clone.rename(columns={'Balance_num':'Quantity'
                , 'storeId':'storeId_HO', 'storeName':'storeName_HO'}))
            pool_1_to_HO = pd.concat([pool_1_to_HO, pool_1_clone], ignore_index= True)
            dn_to_ho = pd.concat([province_to_HO, pool_1_to_HO], ignore_index= True)

            # Appen deficient goods to pool_2_remain -> Goods is still deficient
            pool_2_remain = pd.concat([pool_2_remain, drop_sub_columns_pool2(pool_2_clone)], ignore_index= True)
            dn_remain = pd.concat([province_remain, pool_2_remain], ignore_index= True)

        elif list(data.iterrows())[0][1]['Area'] == "MiềnĐông":
            # Initialize pool_1 & pool_2 clone
            pool_1_clone = pool_1
            pool_2_clone = pool_2
            md_to_ho = pd.DataFrame()
            md_remain = pd.DataFrame()

            # Append residual goods to dataframe pool_1_to_HO from pool_clone -> Goods is still residual
            pool_1_clone = drop_sub_columns_pool1(pool_1_clone.rename(columns={'Balance_num':'Quantity'
                , 'storeId':'storeId_HO', 'storeName':'storeName_HO'}))
            pool_1_to_HO = pd.concat([pool_1_to_HO, pool_1_clone], ignore_index= True)
            md_to_ho = pd.concat([province_to_HO, pool_1_to_HO], ignore_index= True)

            # Appen deficient goods to pool_2_remain -> Goods is still deficient
            pool_2_remain = pd.concat([pool_2_remain, drop_sub_columns_pool2(pool_2_clone)], ignore_index= True)
            md_remain = pd.concat([province_remain, pool_2_remain], ignore_index= True)

        elif list(data.iterrows())[0][1]['Area'] == "MiềnTây":
            # Initialize pool_1 & pool_2 clone
            pool_1_clone = pool_1
            pool_2_clone = pool_2
            mt_to_ho = pd.DataFrame()
            mt_remain = pd.DataFrame()

            # Append residual goods to dataframe pool_1_to_HO from pool_clone -> Goods is still residual
            pool_1_clone = drop_sub_columns_pool1(pool_1_clone.rename(columns={'Balance_num':'Quantity'
                , 'storeId':'storeId_HO', 'storeName':'storeName_HO'}))
            pool_1_to_HO = pd.concat([pool_1_to_HO, pool_1_clone], ignore_index= True)
            mt_to_ho = pd.concat([province_to_HO, pool_1_to_HO], ignore_index= True)

            # Appen deficient goods to pool_2_remain -> Goods is still deficient
            pool_2_remain = pd.concat([pool_2_remain, drop_sub_columns_pool2(pool_2_clone)], ignore_index= True)
            mt_remain = pd.concat([province_remain, pool_2_remain], ignore_index= True)

        else:
            break


        ## FINISH Round 1

    province_to_HO = pd.concat([province_to_HO, md_to_ho, mt_to_ho, dn_to_ho], ignore_index= True)
    province_remain = pd.concat([province_remain, md_remain, mt_remain, dn_remain], ignore_index= True)

    return hn_trans, hn_to_HO, hn_remain, hcm_trans, hcm_to_HO, hcm_remain, province_to_HO, province_remain


## def round_2
def round2(df_1, df_2):
    hn_trans, hn_to_HO, hn_remain, hcm_trans, hcm_to_HO, hcm_remain, province_to_HO, province_remain = round1(df_2)
    # Create & process dataframe store HO goods after receive residual Goods after Round_1
    hcm_to_HO = hcm_to_HO.rename(columns={'storeId_HO': 'storeId', 'storeName_HO': 'storeName', 'Quantity':'StockQuantity'})
    hn_to_HO = hn_to_HO.rename(columns={'storeId_HO': 'storeId', 'storeName_HO': 'storeName', 'Quantity':'StockQuantity'})
    province_to_HO = province_to_HO.rename(columns={'storeId_HO': 'storeId', 'storeName_HO': 'storeName', 'Quantity':'StockQuantity'})
    
    ho_goods = pd.concat([df_1, hn_to_HO, hcm_to_HO, province_to_HO], ignore_index= True)
    ho_goods = ho_goods.groupby(by=['Cat2022','productId', 'productName'])['StockQuantity'].sum()
    ho_goods = ho_goods.reset_index()
    ho_goods = ho_goods[ho_goods['StockQuantity'] > 0]
    ho_goods = ho_goods.reset_index()
    ho_goods['storeId'] = df_1['storeId'].iloc[0]
    ho_goods['storeName'] = df_1['storeName'].iloc[0]
    ho_goods['Area'] = df_1['Area'].iloc[0]
    ho_goods['SO1'] = 0
    ho_goods['SO2'] = 0
    ho_goods['SO3'] = 0
    ho_goods['SO4'] = 0
    ho_goods['AvgSO'] = 0
    ho_goods = ho_goods.reindex(columns=['Area', 'storeId', 'storeName', 'Cat2022', 'productId', 'productName', 'SO1', 'SO2', 'SO3', 'SO4', 'AvgSO', 'StockQuantity'])

    # Combine ho_goods data and pool_2_remain (deficient goods) into 1 dataframe
    round2_data = pd.concat([ho_goods, hn_remain, hcm_remain, province_remain], ignore_index= True)
    round2_data['AvgSO'] = round2_data['AvgSO'].clip(lower=0)
    round2_data['SellPower'] = round(round2_data['AvgSO'].div(7),3)
    round2_data['DOS'] = round(round2_data['StockQuantity']/ round2_data['SellPower'],0)
    round2_data = round2_data.sort_values(by = ['DOS'], ascending= False)
    round2_data['Statement'] = round2_data['DOS'].apply(lambda x: DOS_Classify(x))
    round2_data['Balance_num'] = round2_data.apply(lambda row: cal_balance_num(row['Statement'], row['StockQuantity'], row['SellPower']), axis = 1)
    round2_data = round2_data[round2_data['StockQuantity'] > 0]

    # Create pool_1 & pool_2 in round2_data
    pool_1 = sort_by_balance(round2_data[round2_data['Statement'] == 'residual'])

    pool_2 = sort_by_balance(round2_data[round2_data['Statement'] == 'deficient'])

    # Initialize cache trans
    cache_trans = pool_2[pool_2['productId'] == pool_1.iloc[0]['productId']]

    # Initialize transactions dataframe
    transactions = pd.DataFrame()

    # Initialize pool_3 to store residual in HO
    pool_3 = pd.DataFrame()

    ## Start While loop:
    while True:
        # The amount of goods exceeded pool_1
        n   =   pool_1.iloc[0]['Balance_num']

        # resolve case when ca_transaction is blank
        if len(cache_trans.index) == 0  :
            d = {'storeId_HO':  pool_1.iloc[0]['storeId'],
                 'storeName_HO': pool_1.iloc[0]['storeName'],
                 'productId':          pool_1.iloc[0]['productId'],
                 'productName':        pool_1.iloc[0]['productName'],
                 'Quantity':           n}

            # update pool_3_stay_HO
            pool_3 = pd.concat([pool_3, pd.DataFrame(data = d,index=[0])], ignore_index = True)

            # update pool_1 to select another items
            pool_1.drop(index = pool_1.iloc[0:1,:].index, inplace = True)

            # condition to break loop
            if len(pool_1.index) == 0:
                break

            # upadte cache_transaction
            cache_trans = pool_2.loc[pool_2['productId'] == pool_1.iloc[0]['productId']]

            continue

        # The amount of goods lacked pool_2
        m   =   cache_trans.iloc[0]['Balance_num']

        ## Case : if the exceed > lack
        if  n    >=   m :

            remain_amount = n - m
            # This dataframe is storing the transaction
            translog_dict = {'storeId_transfer': pool_1.iloc[0]['storeId'],
                 'storeName_transfer': pool_1.iloc[0]['storeName'],
                 'storeId_receive': cache_trans.iloc[0]['storeId'],
                 'storeName_receive': cache_trans.iloc[0]['storeName'],
                 'Cat2022' : pool_1.iloc[0]['Cat2022'],
                 'productId': pool_1.iloc[0]['productId'],
                 'productName': pool_1.iloc[0]['productName'], 'Quantity': m}

            # create transaction dataframe
            transactions = pd.concat([transactions,pd.DataFrame(data = translog_dict, index=[0])], ignore_index = False)

            #   Migrate the value
            pool_1.loc[pool_1.index[0], ['Balance_num']] = remain_amount

            pool_2.drop(index=cache_trans.iloc[0:1,:].index, inplace=True)

            #   Refresh
            pool_1 = sort_by_balance(pool_1)
            pool_2 = sort_by_balance(pool_2)

            #   Update cache_transaction
            if len(pool_2.index)==0:
                break
            cache_trans=pool_2.loc[pool_2['productId'] == pool_1.iloc[0]['productId']]
            continue


        # Case 2: Receive quantity < Transfer quantity
        elif m > n and (n != 0 or n != None):
            remain_amount = m - n

            # Intialize dict to store log of transaction
            translog_dict = {'storeId_transfer': pool_1.iloc[0]['storeId'],
                             'storeName_transfer': pool_1.iloc[0]['storeName'],
                             'storeId_receive':    cache_trans.iloc[0]['storeId'],
                             'storeName_receive':  cache_trans.iloc[0]['storeName'],
                             'Cat2022' : pool_1.iloc[0]['Cat2022'],
                             'productId':          pool_1.iloc[0]['productId'],
                             'productName':        pool_1.iloc[0]['productName'], 'Quantity': n}
            # Add logs to transactions dataframe
            transactions = pd.concat([transactions, pd.DataFrame(data= translog_dict, index=[0])], ignore_index= False)

            # update balance_num in pool_2_clone
            pool_2.loc[cache_trans.index[0],['Balance_num']] = remain_amount

            # update stock in pool_2_clone (to run Round 2: HO to Areas) ***
            pool_2.loc[cache_trans.index[0],['StockQuantity']] = pool_2.loc[cache_trans.index[0],['StockQuantity']] + n

            # drop goods is over in pool_1
            pool_1.drop(index = pool_1.iloc[0:1, :].index, inplace = True)

            # Re-sorting pool
            pool_1 = sort_by_balance(pool_1)
            pool_2 = sort_by_balance(pool_2)

            # BREAK when goods in pool_1_clone is over
            if len(pool_1.index)==0:
                break

            # Update cache_trans and continue loop
            cache_trans = pool_2[pool_2['productId']==pool_1.iloc[0]['productId']]
            continue

            ## Case 3: Receive > Transfer (m > n) (1 - 0 = 1, case when n = 0 or n = None)
        elif m > n and (n == 0 or n == None):
            # remain_amount = 0
            # m does not have chane => don't need to update pool_2_clone

            # drop n in pool_1_clone
            pool_1.drop(index=pool_1.iloc[0:1,:].index ,inplace=True)

            # Re-sorting pool
            pool_1 = sort_by_balance(pool_1)
            pool_2 = sort_by_balance(pool_2)

            # BREAK when pool_1_clone is over
            if len(pool_1.index) == 0:
                break

            # Update cache_trans and continue loop
            cache_trans = pool_2[pool_2['productId'] == pool_1.iloc[0]['productId']]
            continue

        else:
            break

    # transactions from HO to Stores
    transactions = transactions[transactions['Quantity'] > 0]
    # Goods still residual in HO
    pool_3 = pool_3[pool_3['Quantity'] > 0]
    # Goods still deficient in HO and other stores
    pool_2 = pool_2[pool_2['Balance_num'] > 0]

    return transactions, hn_trans, hn_to_HO, hcm_trans, hcm_to_HO, province_to_HO, ho_goods, pool_3


# def round_3 to processing final_stock DataFrame
def round3(df, df_1, df_2):
    HO_trans, hn_trans, hn_to_HO, hcm_trans, hcm_to_HO, province_to_HO, ho_goods, ho_residual_goods = round2(df_1, df_2)
    total_trans = pd.concat([hcm_trans, hn_trans], ignore_index= True)
    total_trans['receive_id'] = total_trans.apply(lambda row: str(row['storeId_receive']) + str(row['productId']), axis =1)
    receive_trans = total_trans.groupby(by='receive_id', axis = 0,  as_index = False).sum()
    total_trans['transfer_id'] = total_trans.apply(lambda row: str(row['storeId_transfer']) + str(row['productId']), axis =1)
    transfer_trans = total_trans.groupby(by='transfer_id', axis = 0, as_index= False).sum()

    total_trans_to_HO = pd.concat([hcm_to_HO, hn_to_HO, province_to_HO], ignore_index= True)
    total_trans_to_HO['transfer_id'] = total_trans_to_HO.apply(lambda row: str(row['storeId']) + str(row['productId']), axis =1)
    total_trans_to_HO = total_trans_to_HO.rename(columns={'StockQuantity':'Quantity'})

    HO_trans['receive_id'] = HO_trans.apply(lambda row: str(row['storeId_receive']) + str(row['productId']), axis = 1)
    HO_trans = HO_trans.groupby(by='receive_id', axis = 0, as_index= False).sum()

    df = df.fillna(0)
    df['unique_id'] = df.apply(lambda row: str(row['storeId']) + str(row['productId']), axis =1)

    # Merge receivce quantity in Round_1 (internal transactions)
    df = df.merge(receive_trans.loc[:, ['receive_id','Quantity']], how = 'left', left_on='unique_id', right_on='receive_id')
    df = df.drop('receive_id', axis = 1)
    df = df.rename(columns = {'Quantity': 'internal_Receive'})

    # Merge transfer quantity in Round_1 (internal transactions)
    df = df.merge(transfer_trans.loc[:,['transfer_id', 'Quantity']], how = 'left', left_on='unique_id', right_on='transfer_id')
    df = df.drop('transfer_id', axis = 1)
    df = df.rename(columns = {'Quantity': 'internal_Transfer'})

    # Merge transfer to HO quantity
    df = df.merge(total_trans_to_HO.loc[:,['transfer_id', 'Quantity']], how ='left', left_on='unique_id', right_on='transfer_id')
    df = df.drop('transfer_id', axis = 1)
    df = df.rename(columns = {'Quantity': 'back_to_HO'})

    # Merge quantity tranfer from HO to strores
    df = df.merge(HO_trans.loc[:,['receive_id', 'Quantity']], how='left', left_on='unique_id', right_on='receive_id')
    df = df.drop('receive_id', axis = 1)
    df = df.rename(columns={'Quantity': 'HO_tranfer'})

    # Last processing
    df = df.drop('unique_id', axis = 1)
    df = df.fillna(0)
    df['final_Qty'] = df['StockQuantity'] + df['internal_Receive'] - df['internal_Transfer'] \
                      - df['back_to_HO'] + df['HO_tranfer']
    df['SellPower'] = round(df['AvgSO'].div(7),3)
    df['final_DOS'] = round(df['final_Qty']/df['SellPower'],0)
    df['final_status'] = df['final_DOS'].apply(lambda x: DOS_Classify(x))
    df['PO_Qty'] = df.apply(lambda row: cal_balance_num(row['final_status'], row['final_Qty'], row['SellPower']), axis = 1)
    df['final_DOS'] =df['final_DOS'].fillna(np.inf)
    df['final_status'] = df['final_DOS'].apply(lambda x: 'Không có Sức bán' if x == np.inf else ('Thiếu' if x < 26 else 'Đủ'))
    df = df.drop('SellPower', axis = 1)

    #Export to excel files
    with pd.ExcelWriter(f"{path}Demo_Goods.xlsx") as writer:
        df.to_excel(writer, sheet_name='final_stocks')
        hcm_trans.to_excel(writer, sheet_name='HCM_trans')
        hn_trans.to_excel(writer, sheet_name='HN_trans')
        total_trans_to_HO.to_excel(writer, sheet_name = 'total_Back_HO')
        HO_trans.to_excel(writer, sheet_name='HO_trans')
        ho_residual_goods.to_excel(writer, sheet_name = 'HO_final')
    #return df, hcm_trans, hn_trans, total_trans_to_HO, HO_trans, ho_residual_goods


# define df_2 copy of df (main stocks's df)
df_2 = df[['Area','storeId','storeName', 'Cat2022', 'productId','productName'
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

# call all functions
round3(df_0, df_1, df_2)



### Round 4 - Distributing with imeis
## Round 4.1 => Internal distribution in HCM, HN
# 4.1.1 - Load imei's stocks in (HCM, HN), load internal trans in (HCM, HN)
# 4.1.2 - Creating id_transfer (storeId + productId),Sorting ASC by importDate
# 4.1.3 - Using for loop with interal trans to fill imeis => List imeis (internal trans)

## Round 4.2 - Distributing imeis to back to HO
# 4.2.1 - Load imei's still in all areas (combine others & hn,hcm), load imeis stock in HO (MGS, DP_2) + rename 'id_transfer'
# 4.2.2 - Using for loop in transactions of imeis back to HO, initialize storeId_receive, storeName_receive => List imeis (back to HO)

## Round 4.3 - Distributing imeis from HO to stores
# 4.3.1 - Load HO_stock_imes and concat with imei_back_HO => New HO_imei_stocks + create id_transfer
# 4.3.2 - Load HO to Stores transactions + create id_transfer
# 4.3.3 - Running for loop in trans of HO to Stores, initialize storeId_receive, storeName_receive => List imeis (back to HO)

### inititalize dataframe contains imei_stocks, ho_imei_stocks
df_3 = pd.read_excel(f'{path}Stocks_IMEI.xlsx') # imeis in areas
df_4 = pd.read_excel(f'{path}HO_Stock_IMEI.xlsx') # imeis in HO

# initialize function to sort by importDate and creating id_transfer
def sort_by_import(df):
    df = df.sort_values(by=['storeId', 'productId', 'importDate'], ascending = True).reset_index(drop= True)
    df['transfer_id'] = df.apply(lambda row: str(row['storeId']) + str(row['productId']), axis = 1)
    return df

list_imei = sort_by_import(df_3)
HO_list_imei = sort_by_import(df_4)

def round4(df_1,df_2, df_3, df_4):
    HO_trans, hn_trans, hn_to_HO, hcm_trans, hcm_to_HO, province_to_HO, ho_goods, ho_residual_goods = round2(df_1, df_2)
    internal_trans = pd.concat([hcm_trans, hn_trans], axis= 0, ignore_index= True)
    internal_trans['transfer_id'] = internal_trans.apply(lambda row: str(row['storeId_transfer']) + str(row['productId']), axis= 1)
    back_HO_trans = pd.concat([hcm_to_HO, hn_to_HO, province_to_HO], axis= 0, ignore_index= True)
    back_HO_trans['transfer_id'] = back_HO_trans.apply(lambda row: str(row['storeId']) + str(row['productId']), axis= 1)
    back_HO_trans = back_HO_trans.rename(columns={'StockQuantity':'Quantity'})
    HO_trans_store = HO_trans
    HO_trans_store['transfer_id'] = HO_trans_store.apply(lambda row: str(row['storeId_transfer']) + str(row['productId']), axis= 1)
    
    # Round 4.1
    internal_trans_imei_list = pd.DataFrame()
    for i in range(len(internal_trans.index)):
        id_trans = internal_trans.iloc[i]['transfer_id']
        num_trans = int(internal_trans.iloc[i]['Quantity'])
        imei_trans = df_3[df_3['transfer_id'] == str(id_trans)].reset_index(drop= True)
        imei_trans_log = imei_trans.iloc[0: num_trans]
        imei_trans_log = imei_trans_log.merge(internal_trans.iloc[i:i+1][['storeId_receive', 'storeName_receive','transfer_id']], how= 'inner', on= 'transfer_id')
        
        # drop imei transfered in stocks
        list_imei_drop = imei_trans_log['imeiNo'].tolist()
        df_3 = df_3[~df_3['imeiNo'].isin(list_imei_drop)].reset_index(drop=True)
        
        # concat list of transfer
        internal_trans_imei_list = pd.concat([internal_trans_imei_list, imei_trans_log], axis= 0, ignore_index= True)
        
    # Re-Arrange internal_trans_imei_list
    internal_trans_imei_list = internal_trans_imei_list[['Area', 'storeId', 'storeName', 'storeId_receive', 'storeName_receive', 'CatTarget','productId', 'productName', 'imeiNo', 'importDate', 'StockQuantity']]
    
    # Round 4.2
    # df_3 <> still imeis remaining in total areas
    list_imei_back_HO = pd.DataFrame()
    for i in range(len(back_HO_trans)):
        id_trans = back_HO_trans.iloc[i]['transfer_id']
        num_trans = int(back_HO_trans.iloc[i]['Quantity'])
        imei_trans = df_3[df_3['transfer_id'] == str(id_trans)].reset_index(drop= True)
        imei_trans_log = imei_trans.iloc[0: num_trans]
        
        # drop imei transfered in stocks
        list_imei_drop = imei_trans_log['imeiNo'].tolist()
        df_3 = df_3[~df_3['imeiNo'].isin(list_imei_drop)].reset_index(drop=True)
        
        # concat list_imei_back_HO
        list_imei_back_HO = pd.concat([list_imei_back_HO, imei_trans_log], axis =0, ignore_index =True)
        
    # Processing list_imei_back_HO (storeId_receive, storeName_receive)
    list_imei_back_HO['storeId_receive'] = df_4.iloc[0]['storeId']
    list_imei_back_HO['storeName_receive'] = df_4.iloc[0]['storeName']
    list_imei_back_HO = list_imei_back_HO[['Area', 'storeId', 'storeName', 'storeId_receive', 'storeName_receive', 'CatTarget','productId', 'productName', 'imeiNo', 'importDate', 'StockQuantity']]
    
    # Round 4.3
    # df stores imeis merge to HO_stock_imei
    concat_df = list_imei_back_HO[['Area', 'storeId_receive', 'storeName_receive', 'CatTarget','productId', 'productName', 'imeiNo', 'importDate', 'StockQuantity']]
    concat_df = concat_df.rename(columns={"storeId_receive":"storeId", "storeName_receive":"storeName"})
    
    AF1_HO_stock = pd.concat([df_4, concat_df], axis= 0, ignore_index= True)
    AF1_HO_stock['transfer_id'] = AF1_HO_stock.apply(lambda row: str(row['storeId']) + str(row['productId']), axis= 1)
    AF1_HO_stock = sort_by_import(AF1_HO_stock)
    
    # initialize df to stores transactions with imeis 
    list_imei_HO_trans = pd.DataFrame()
    for i in range(len(HO_trans_store.index)):
        id_trans = HO_trans_store.iloc[i]['transfer_id']
        nums_trans = int(HO_trans_store.iloc[i]['Quantity'])
        imei_trans = AF1_HO_stock[AF1_HO_stock['transfer_id'] == id_trans].reset_index(drop= True)
        imei_trans_logs = imei_trans.iloc[0: nums_trans]
        #imei_trans_logs = pd.concat([imei_trans_logs,df_5.iloc[i:i+1][['storeId_receive', 'storeName_receive']]], axis = 1, ignore_index = True)
        imei_trans_logs = imei_trans_logs.merge(HO_trans_store.iloc[i:i+1][['storeId_receive', 'storeName_receive','transfer_id']], how = 'inner', on='transfer_id')
        
        # drop in imeiStock
        list_imei_drop = imei_trans_logs['imeiNo'].tolist()
        AF1_HO_stock = AF1_HO_stock[~AF1_HO_stock['imeiNo'].isin(list_imei_drop)].reset_index(drop= True)
        
        # concat list imei trans
        list_imei_HO_trans = pd.concat([list_imei_HO_trans, imei_trans_logs], axis =0, ignore_index= True)
    
    list_imei_HO_trans = list_imei_HO_trans[['Area', 'storeId', 'storeName', 'storeId_receive', 'storeName_receive', 'CatTarget','productId', 'productName', 'imeiNo', 'importDate', 'StockQuantity']]
    
    # Export 3 lists of imeis to excel file
    #return internal_trans_imei_list, list_imei_back_HO, list_imei_HO_trans
    with pd.ExcelWriter(f"{path}Demo_Goods.xlsx", mode="a", engine="openpyxl") as writer:
        internal_trans_imei_list.to_excel(writer, sheet_name='HCM_HN_imei_trans')
        list_imei_back_HO.to_excel(writer, sheet_name='Back_HO_imei_trans')
        list_imei_HO_trans.to_excel(writer, sheet_name='HO_trans_store_imei')
        
    

#df_3_0, hcm_trans, hn_trans, total_trans_to_HO, HO_trans, ho_residual_goods = round3(df_0, df_1, df_2)
#internal_trans_imei_list, list_imei_back_HO, list_imei_HO_trans = round4(df_1, df_2, list_imei, HO_list_imei)
round4(df_1, df_2, list_imei, HO_list_imei)

# with pd.ExcelWriter(f"{path}Demo_Goods.xlsx") as writer:
#     df_3_0.to_excel(writer, sheet_name='final_stocks')
#     hcm_trans.to_excel(writer, sheet_name='HCM_trans')
#     hn_trans.to_excel(writer, sheet_name='HN_trans')
#     total_trans_to_HO.to_excel(writer, sheet_name = 'total_Back_HO')
#     HO_trans.to_excel(writer, sheet_name='HO_trans')
#     ho_residual_goods.to_excel(writer, sheet_name = 'HO_final')
#     internal_trans_imei_list.to_excel(writer, sheet_name='HCM_HN_imei_trans')
#     list_imei_back_HO.to_excel(writer, sheet_name='Back_HO_imei_trans')
#     list_imei_HO_trans.to_excel(writer, sheet_name='HO_trans_store_imei')
    

    
    








