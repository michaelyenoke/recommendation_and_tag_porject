#!/usr/bin/env python
# coding: utf-8

# # Application 

# In[ ]:


recommendation_final(memeber_id)


# ------

# # Process

# In[1]:


import psycopg2
import sys
import os
import time


# In[2]:


import pandas as pd 
from decimal import *
import datetime


# In[3]:


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


# ------

# # Queries

# In[6]:


class all_the_query_works:
    def __init__(self):
        pass

    def query_for_member_declare_sport(self):
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
        member_declare_sport_result = execute_query(member_declare_sport)
        member_declare_sport_result_df = pd.DataFrame(member_declare_sport_result)
        member_declare_sport_result_df.columns=['loyalty_card_num','sport_id','sport_name_zh','sport_name_en','classificaiton_code']
        return member_declare_sport_result_df
    


# In[7]:


class all_the_query_works_2:
    def __init__(self):
        pass

    def query_for_prod_recom_by_sport_id(self):
        sport_top_sale_by_brand= '''
        select *
        from dtm_tw.taiwan_cdp_tag_product_sport_type
        '''
        sport_top_sale_by_brand_result = execute_query(sport_top_sale_by_brand)
        sport_top_sale_by_brand_result_df = pd.DataFrame(sport_top_sale_by_brand_result)
        sport_top_sale_by_brand_result_df.columns=['count','brand_id','brand_name','model_code_r3','web_label','sport_type']
        return sport_top_sale_by_brand_result_df


# In[8]:


class all_the_query_works_3:
    def __init__(self):
        pass

    def query_for_sport_id_model_r3(self):
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
        return sport_id_model_r3_result_df


# # Recommendation 01

# In[9]:


class  member_declare_and_tag_type:
    global member_declare_sport_result_df_table
    query_for_member_declare = all_the_query_works().query_for_member_declare_sport
    member_declare_sport_result_df_table = query_for_member_declare()
    
    def __init__(self):
        pass
    
    def member_declare(self,member_id):
        spid = (member_declare_sport_result_df_table[member_declare_sport_result_df_table['loyalty_card_num'] == str(member_id)])
        #轉成list
        spid_list = list(set(spid['sport_name_en']))
        return spid_list
    

    def member_declare_type(self,member_id):
        cfcode = (member_declare_sport_result_df_table[member_declare_sport_result_df_table['loyalty_card_num'] == str(member_id)])
        cfcode_list = list(set(cfcode['classificaiton_code']))
        cfcode_list_random = random.choice(cfcode_list)
        cfcode_list_random_int = list(map(int,cfcode_list_random))
        cfcode_list_random_int_num = cfcode_list_random_int[0]    
        return cfcode_list_random_int_num
    


# In[10]:


class sport_type_and_product_recommendation:
    global prodcut_q_table
    query_prod_rec = all_the_query_works_2().query_for_prod_recom_by_sport_id
    prodcut_q_table = query_prod_rec()
    global member_declare_type_2
    global member_declare_2
    member_declare_type_2 = member_declare_and_tag_type().member_declare_type
    member_declare_2 = member_declare_and_tag_type().member_declare  
    
    
    def __init__(self):
        self.sport01 = [['Camping',9295],['Hiking',9295],['Trekking',1335]]
        self.sport02 = [['Jogging',9313],['Gym',9251],['Pilates',2191],['Cross Training',9251],['Training',2191],['Cardio',2191]]
        self.sport03 = [['Basketball',1374],['Badmintion',1688]]
        self.sport04 = [['Swimming',8504],['Fishing',14515],['Biking',20153],['Dart',1525],['Walking',9366]]
    
    
    def sport_random_by_claim(self,sport_num):
         if sport_num == 1: 
            sport01_random = random.choice(self.sport01)
            return sport01_random
         elif sport_num == 2:
            sport02_random = random.choice(self.sport02)
            return sport02_random   
         elif sport_num == 3:
            sport03_random = random.choice(self.sport03)
            return sport03_random   
         elif sport_num == 4:
            sport04_random = random.choice(self.sport04)
            return sport04_random   
        
    
    def prod_recom_by_sport_id(self,bra_id):
        a = random.randint(0,19)
        b = random.randint(0,19)
        product_recommendation_by_type = prodcut_q_table[prodcut_q_table['brand_id'] == str(bra_id)]
        sport_type = product_recommendation_by_type['sport_type']
        brand_id = product_recommendation_by_type['brand_id']
        model_r3 = product_recommendation_by_type['model_code_r3']
        web_label = product_recommendation_by_type['web_label']
        product_recommendation_by_type_concat = pd.concat([sport_type,brand_id,model_r3,web_label], axis=1) 
        product_recommendation_by_type_concat.index = range(len(product_recommendation_by_type_concat))
        product_recommendation_by_type_concat.columns=['','','','']
        recom = product_recommendation_by_type_concat.loc[a:a+1]
        print("The Product recommend to you is %s" % (recom))  

        
    # 根據宣稱下選擇一個隨機運動然後進行推薦    
    def potential_sport_tag_and_recommendation(self,member_id) :
        declare =  member_declare_2(member_id)
        print("Your decalred sports :  %s" % (declare))
        sp_type=sport_type_and_product_recommendation()
        member_type = member_declare_type_2(member_id)
        if member_type == 1:
            potential = sp_type.sport_random_by_claim(4)
            bra_id = potential[1]
            print("The POTENTIAL random sport to recommend to you is %s" % (potential[0]) )
            sp_type.prod_recom_by_sport_id(bra_id)
        elif member_type == 2:
            potential = sp_type.sport_random_by_claim(2)
            bra_id = potential[1]
            print("The POTENTIAL random sport to recommend to you is %s" % (potential[0]) )
            sp_type.prod_recom_by_sport_id(bra_id)
        elif member_type == 3:
            potential = sp_type.sport_random_by_claim(2)
            bra_id = potential[1]
            print("The POTENTIAL random sport to recommend to you is %s" % (potential[0]) )
            sp_type.prod_recom_by_sport_id(bra_id) 
        elif member_type == 4:
            potential = sp_type.sport_random_by_claim(1)
            bra_id = potential[1]
            print("The POTENTIAL random sport to recommend to you is %s" %  (potential[0]) )
            sp_type.prod_recom_by_sport_id(bra_id)
        else:
            potential = sp_type.sport_random_by_claim(4)
            bra_id = potential[1]
            print("The POTENTIAL random sport to recommend to you is %s" %  (potential[0]) )
            sp_type.prod_recom_by_sport_id(bra_id)
            


# # Recommendation 02

# In[11]:


from datetime import datetime
import calendar


# In[12]:


class recommendation_by_newest_product:
    global member_declare_type_2
    member_declare_type_2 = member_declare_and_tag_type().member_declare_type
    global member_declare_2
    member_declare_2 = member_declare_and_tag_type().member_declare
    global sport_id_model_r3_result_df_table
    sport_id_model_r3_result_df_funciton = all_the_query_works_3().query_for_sport_id_model_r3
    sport_id_model_r3_result_df_table = sport_id_model_r3_result_df_funciton()
    
    def __init__(self):
        pass
    
    
    def member_declare_random_sport(self,member_id):
        spid = (member_declare_sport_result_df_table[member_declare_sport_result_df_table['loyalty_card_num'] == str(member_id)])
        one_random_sport = list(set(spid['sport_id']))
        one_random_sport_result = random.choice(one_random_sport)
        return one_random_sport_result
    

    def week_num(self):
        cal = datetime.today().isocalendar() 
        strcal = str(cal[0])+str(cal[1]) 
        return strcal 
    
    

    def recom_prod_new_2(self,member_id):
        one_random_sport_result = recommendation_by_newest_product().member_declare_random_sport
        this_week_num = recommendation_by_newest_product().week_num
        strcal = this_week_num()
        recom_prod_new = sport_id_model_r3_result_df_table.loc[(sport_id_model_r3_result_df_table['sports_id'] == one_random_sport_result) & (sport_id_model_r3_result_df_table['p_sell_start_week'] <= strcal)]
        if recom_prod_new.empty:
            return recom_prod_new
        else:
            uu = max(recom_prod_new['p_sell_start_week'])
            recom_prod_new_2 = sport_id_model_r3_result_df_table.loc[(sport_id_model_r3_result_df_table['sports_id'] == one_random_sport_result) & (sport_id_model_r3_result_df_table['p_sell_start_week'] == uu)]
            return recom_prod_new_2

    def new_prod_recommendation(self,member_id): 
        recom_prod = recommendation_by_newest_product().recom_prod_new_2
        recom_prod_new_3 = recom_prod(member_id)
        if recom_prod_new_3.empty:
            print('No New Products')
        else:
            recom_prod_new_3 = recom_prod(member_id)
            if len(recom_prod_new_3) != 1 :
                a = random.randint(0,len(recom_prod_new_3))
                recom_prod_new_3.index = range(len(recom_prod_new_3))
                yy = recom_prod_new_3.iloc[a:a+3]
                return yy
            elif len(recom_prod_new_3) == 1:
                a = random.randint(0,len(recom_prod_new_3))
                recom_prod_new_3.index = range(len(recom_prod_new_3))
                kk = recom_prod_new_3.iloc[0:1]
                return kk
            elif len(recom_prod_new_3) == 0:
                a = random.randint(0,len(recom_prod_new_3))
                recom_prod_new_3.index = range(len(recom_prod_new_3))
                gg = recom_prod_new_3.iloc[0:0]
                return gg


# # __name__ == "__main__"

# In[13]:


if __name__ == "__main__":
    def recommendation_final(member_id):
        recom_result = sport_type_and_product_recommendation() # Recommendation 01
        p1 = recom_result.potential_sport_tag_and_recommendation(member_id)
        recom_newest=recommendation_by_newest_product() # Recommendation 02
        p2 = recom_newest.new_prod_recommendation(member_id)    
        print(p1)
        print('@ the newest product to recommend to you:')
        print(p2)

