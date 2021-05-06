#!/usr/bin/env python
# coding: utf-8

# # Application

# In[22]:


new_prod_recommendation(2091200208332)


# In[95]:


# random loyalty card number _ 隨機選取有用的會員卡號
ran_member = random.choice(member_declare_sport_result_df['loyalty_card_num'])
print("The random loyalty card number is :" +" "+ran_member)
new_prod_recommendation(ran_member)


# # Process

# In[1]:


import psycopg2
import sys
import os
import time


# In[3]:


import pandas as pd 
from decimal import *
import datetime
import random


# In[4]:


# 執行 query 的 function
def execute_query(query):
    try:
        conn = psycopg2.connect(dbname=DatabaseName, host=ClusterEndpoint, port=Port, user=UserName, password=Password)
        cur = conn.cursor()
        start = time.time()
        cur.execute(query)
        end = time.time()
        print('Time Taken in Seconds : '+str(round(end - start)))
        records = cur.fetchall()
        cur.close()
        conn.commit()
    except Exception as e:
       s = "UNEXPECTED ERROR: " + str(sys.exc_info())+"\n"+str(e)+"\nError on line {}".format(sys.exc_info()[-1].tb_lineno)
       print(s)
    finally:  
       conn.close()
    return records


# In[5]:


# 之後 table 要改成 VIEW


# # Declare Sport

# In[6]:


# Declare Sport
member_declare_sport = '''
SELECT t1.loyalty_card_num,
       t1.sport_id,
       t1.sport_name_zh,
       t1.sport_name_en,
       t2.classificaiton_code
FROM dtm_tw.tw_declare_sport AS t1
left join dtm_tw.taiwan_cdp_sport_tag AS t2
on t1.sport_id = t2.sport_id
where t1.sport_id != 'None'
group by 1,2,3,4,5
;
 '''
# Execut_SQL_Code_01
member_declare_sport_result = execute_query(member_declare_sport)
member_declare_sport_result_df = pd.DataFrame(member_declare_sport_result)
member_declare_sport_result_df.columns=['loyalty_card_num','sport_id','sport_name_zh','sport_name_en','classificaiton_code']
#print(member_declare_sport_result_df)


# In[7]:


# 你宣稱的運動_輸入你的會員就會出現你宣稱的運動_用在輸出總結果的時候看你宣稱的運動
# 這裡改成 sport_id

def member_declare_random_sport(member_id):
    #print(query_result_df[query_result_df['loyalty_card_num'] == str(x)])
    spid = (member_declare_sport_result_df[member_declare_sport_result_df['loyalty_card_num'] == str(member_id)])
    #轉成list
    one_random_sport = list(set(spid['sport_id']))
    #print(one_random_sport)
    one_random_sport_result = random.choice(one_random_sport)
    return one_random_sport_result


# In[8]:


#one_random_sport_result = member_declare_random_sport(2092202984569)
#print(one_random_sport_result)


# # Use the created  table for sport_id & model_code_r3 & p_sell_start_week

# In[9]:


sport_id_model_r3 = '''
SELECT distinct
    model_code_r3,  
    brand_id, 
    brand_name,
    p_sell_start_week, 
    web_label,      
    sports_id 
FROM dtm_tw.taiwan_cdp_sport_id_model_r3
WHERE p_sell_start_week > 202100
 ;
 ''' 

sport_id_model_r3_result = execute_query(sport_id_model_r3)
sport_id_model_r3_result_df = pd.DataFrame(sport_id_model_r3_result)
sport_id_model_r3_result_df.columns=['model_code_r3','brand_id','brand_nam','p_sell_start_week','web_label','sports_id']
#print(sport_id_model_r3_result_df)


# # Capture the Week Num

# In[10]:


from datetime import datetime
import calendar


# In[11]:


def week_num():
    cal = datetime.today().isocalendar() # 年,第幾週,第幾天
    strcal = str(cal[0])+str(cal[1]) # 合併字串
    return strcal # 本週週數


# In[12]:


# strcal = week_num()
# print(type(strcal)) # <class 'str'>
# print(strcal)


# # Select New Product Table

# In[13]:


# 用來看 sport_id 和 p_sell_start_week
# sport_id_model_r3_result_df


# # Select Newest Product by sport_id & week_num_this_week

# In[14]:


# 改用sport_id_model_r3 來決定產品的開始日期 p_sell_start_week
# 用 MEMBER 宣稱的運動來選　sport_id & 使用 本週週數來決定最新的產品, 都透過 sport_id_model_r3 來處理


def recom_prod_new_2(member_id):
    # 使用者宣稱的運動
    one_random_sport_result = member_declare_random_sport(member_id)
    # 計算本週週數
    strcal = week_num()
    ##
    # 第一步篩選本週以及小於本週的週數 (<= strcal)
    recom_prod_new = sport_id_model_r3_result_df.loc[(sport_id_model_r3_result_df['sports_id'] == one_random_sport_result) & (sport_id_model_r3_result_df['p_sell_start_week'] <= strcal)]
    # print(type(recom_prod_new)) <class 'pandas.core.frame.DataFrame'>
    # print(recom_prod_new)
    # print(len(recom_prod_new)) # 查看推薦數量有多少
    #　第二步篩選週數最大的（最新的）
    if recom_prod_new.empty:
        return recom_prod_new
    else:
        uu = max(recom_prod_new['p_sell_start_week'])
        # 第三步將最大的值直接作為篩選的值
        recom_prod_new_2 = sport_id_model_r3_result_df.loc[(sport_id_model_r3_result_df['sports_id'] == one_random_sport_result) & (sport_id_model_r3_result_df['p_sell_start_week'] == uu)]
        # 直接推薦最近的一週 + MEMBER所宣稱的SPORT ID
        return recom_prod_new_2

#####################################################################################################################################################################################

## example
# recom_prod_new =  sport_id_model_r3_result_df.loc[(sport_id_model_r3_result_df['sports_id'] == '134') & (sport_id_model_r3_result_df['p_sell_start_week'] == '202104')]

## seperate workable
#sport_id_model_r3_result_df.loc[sport_id_model_r3_result_df['sports_id'] == one_random_sport_result]
#sport_id_model_r3_result_df.loc[sport_id_model_r3_result_df['p_sell_start_week'] == strcal]


#  # Random Recommend 

# In[15]:


def new_prod_recommendation(member_id):
    recom_prod_new_3 = recom_prod_new_2(member_id)
    if recom_prod_new_3.empty:
        print('No New Products')
    else:
        recom_prod_new_3 = recom_prod_new_2(member_id)
        if len(recom_prod_new_3) != 1 :
            a = random.randint(0,len(recom_prod_new_3))
            #print(a)
            #重新編排欄位
            recom_prod_new_3.index = range(len(recom_prod_new_3))
            #print(recom_prod_new_3)
            #推薦商品
            yy = recom_prod_new_3.iloc[a:a+3]
            return yy
        elif len(recom_prod_new_3) == 1:
            a = random.randint(0,len(recom_prod_new_3))
            #print(a)
            #重新編排欄位
            recom_prod_new_3.index = range(len(recom_prod_new_3))
            #print(recom_prod_new_3)
            #推薦商品
            kk = recom_prod_new_3.iloc[0:1]
            return kk
        elif len(recom_prod_new_3) == 0:
            a = random.randint(0,len(recom_prod_new_3))
            #print(a)
            #重新編排欄位
            recom_prod_new_3.index = range(len(recom_prod_new_3))
            #print(recom_prod_new_3)
            #推薦商品
            gg = recom_prod_new_3.iloc[0:0]
            return gg


# -----
可用的table

## Sheet ##

# spi_cattalog
https://docs.google.com/spreadsheets/d/1Dz6AH3b7IUaane1ujMNtxo5eabGlGqUuzzObccGCL8A/edit#gid=0

# spi_sport_tree
https://docs.google.com/spreadsheets/d/1Dz6AH3b7IUaane1ujMNtxo5eabGlGqUuzzObccGCL8A/edit#gid=724138800



## Slider ##

# ods.spi_sport_tree
https://docs.google.com/presentation/d/1uosPi73c3G7tTXE0TOlQCRK9iukn4kS7y_jChKRDUfQ/edit#slide=id.g33648363b6_6_27