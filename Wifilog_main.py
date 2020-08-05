# coding:utf-8
'''
Created on Jul 14, 2020

@author: mizuno
'''
#import wifilog.Wifilog_lib as mdl
import wifilog.Wifilog_lib as mdl
import pandas as pd
import time

#csv繝輔ぃ繧､繝ｫ縺ｮ蜿悶ｊ霎ｼ縺ｿ
def getCSV(file):
    return pd.read_csv(file, engine='python')

start = time.time()
df_ap = getCSV('./csv/APlocations.csv')
print(df_ap.head()) #譛�蛻昴�ｮ謨ｰ陦後ｒ遒ｺ隱�
print(df_ap.columns) #蛻怜錐繧堤｢ｺ隱�
print(df_ap.dtypes) #蜷�蛻励�ｮ隕∫ｴ�
print(df_ap.describe()) #邨ｱ險磯㍼繧定｡ｨ遉ｺ
print(len(df_ap)) #陦梧焚繧定｡ｨ遉ｺ

df_data = getCSV('./csv/test.csv')
#df_data = getCSV('./csv/2014_01.csv')
print(df_data.head()) #譛�蛻昴�ｮ謨ｰ陦後ｒ遒ｺ隱�
print(df_data.columns) #蛻怜錐繧堤｢ｺ隱�
print(df_data.dtypes) #蜷�蛻励�ｮ隕∫ｴ�
print(df_data.describe()) #邨ｱ險磯㍼繧定｡ｨ遉ｺ
print(len(df_data)) #陦梧焚繧定｡ｨ遉ｺ

elapsed_time = time.time() - start
print ("data_import_time:{0}".format(elapsed_time) + "[sec]")

wlib = mdl.Wifilog_lib(df_ap, df_data)
#驥崎､�繧貞炎髯､ (Mac繧｢繝峨Ξ繧ｹ縺ｧ繝ｦ繝九�ｼ繧ｯ縺ｫ縺吶ｋ)
df_unique = wlib.getDuplicate(df_data, 'client')
print(len(df_unique))

#謗ｨ遘ｻ鬆ｻ蠎ｦ縺ｮ邂怜�ｺ
start = time.time()
wlib.getTransition(df_data, df_unique)
print("time:{}".format(time.time()-start))
transitions = wlib.getTransitionData()

wlib.saveCSV(transitions, './csv/transition.csv')