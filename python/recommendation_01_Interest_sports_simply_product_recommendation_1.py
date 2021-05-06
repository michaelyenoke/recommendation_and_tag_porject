#!/usr/bin/env python
# coding: utf-8

# # Application 


# please key-in any member id which claimed favorite sport
potential_sport(/-----key_in_member_id-----/)


# In[14]:


# random loyalty card number
ran_member = random.choice(member_declare_sport_result_df['loyalty_card_num'])
print("The random loyalty card number is :" +" "+ran_member)
potential_sport(ran_member)



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


# In[4]:


import random


# In[5]:

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


# # Declare Sport

# In[6]:


# SQL_code_01
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

def member_declare(member_id):
    #print(query_result_df[query_result_df['loyalty_card_num'] == str(x)])
    spid = (member_declare_sport_result_df[member_declare_sport_result_df['loyalty_card_num'] == str(member_id)])
    #轉成list
    spid_list = list(set(spid['sport_name_en']))
    return spid_list

#  mds = member_declare(2090538722794)
#  print(mds)
#  ['CANYONING', 'JOGGING', 'SWIMMING']



# In[8]:


def member_declare_type(member_id):
       #print(query_result_df[query_result_df['loyalty_card_num'] == str(x)])
    cfcode = (member_declare_sport_result_df[member_declare_sport_result_df['loyalty_card_num'] == str(member_id)])
    cfcode_list = list(set(cfcode['classificaiton_code']))
    cfcode_list_random = random.choice(cfcode_list)
    cfcode_list_random_int = list(map(int,cfcode_list_random))
    cfcode_list_random_int_num = cfcode_list_random_int[0]    
    return cfcode_list_random_int_num


# In[9]:


def sport01():
    sport01 = [['Camping',9295],['Hiking',9295],['Trekking',1335]]
    sport01_random = random.choice(sport01)
    #print(sport01_random)
    return sport01_random
    
def sport02():    
    sport02 = [['Jogging',9313],['Gym',9251],['Pilates',2191],['Cross Training',9251],['Training',2191],['Cardio',2191]]
    sport02_random = random.choice(sport02)
    #print(sport02_random)
    return sport02_random

def sport03():
    sport03 = [['Basketball',1374],['Badmintion',1688]]
    sport03_random = random.choice(sport03)
    #print(sport03_random)
    return sport03_random

def sport04():   
    sport04 = [['Swimming',8504],['Fishing',14515],['Biking',20153],['Dart',1525],['Walking',9366]]
    sport04_random = random.choice(sport04)
    #print(sport04_random)
    return sport04_random



# #  All product selection & recommendation

# In[10]:

sport_top_sale_by_brand= '''
select *
from dtm_tw.taiwan_cdp_tag_product_sport_type
'''

sport_top_sale_by_brand_result = execute_query(sport_top_sale_by_brand)
sport_top_sale_by_brand_result_df = pd.DataFrame(sport_top_sale_by_brand_result)
sport_top_sale_by_brand_result_df.columns=['count','brand_id','brand_name','model_code_r3','web_label','sport_type']
#print(sport_top_sale_by_brand_result_df)


# In[11]:


def prod_recom(bra_id):
    a = random.randint(0,19)
    b = random.randint(0,19)
    product_recommendation_by_type = sport_top_sale_by_brand_result_df[sport_top_sale_by_brand_result_df['brand_id'] == str(bra_id)]
    sport_type = product_recommendation_by_type['sport_type']
    brand_id = product_recommendation_by_type['brand_id']
    model_r3 = product_recommendation_by_type['model_code_r3']
    web_label = product_recommendation_by_type['web_label']
    product_recommendation_by_type_concat = pd.concat([sport_type,brand_id,model_r3,web_label], axis=1) 
    product_recommendation_by_type_concat.index = range(len(product_recommendation_by_type_concat))
    product_recommendation_by_type_concat.columns=['','','','']
    recom = product_recommendation_by_type_concat.loc[a:a+1]
    print("The Product recommend to you is %s" % (recom))


# # 運動與商品推薦

# In[12]:

def potential_sport(member_id) :
    declare =  member_declare(member_id)
    print("Your decalred sports :  %s" % (declare))
    member_type = member_declare_type(member_id)
    if member_type == 1:
        potential = sport04()
        bra_id = potential[1]
        print("The POTENTIAL random sport to recommend to you is %s" % (potential[0]) )
        prod_recom(bra_id)
    elif member_type == 2:
        potential = sport02()
        bra_id = potential[1]
        print("The POTENTIAL random sport to recommend to you is %s" % (potential[0]) )
        prod_recom(bra_id)
    elif member_type == 3:
        potential = sport02()
        bra_id = potential[1]
        print("The POTENTIAL random sport to recommend to you is %s" % (potential[0]) )
        prod_recom(bra_id) 
    elif member_type == 4:
        potential = sport01()
        bra_id = potential[1]
        print("The POTENTIAL random sport to recommend to you is %s" %  (potential[0]) )
        prod_recom(bra_id)
    else:
        potential = sport04()
        bra_id = potential[1]
        print("The POTENTIAL random sport to recommend to you is %s" %  (potential[0]) )
        prod_recom(bra_id)





