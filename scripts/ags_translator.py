# -*- coding: utf-8 -*-
"""
Created on Sun Oct 01 09:44:45 2017

@author: Achim Tack
"""

#imports
import pandas as pd
import os
import urllib
from  datetime import datetime
import numpy as np


#paths
input_dir = '../data/input/'
tmp_dir = '../data/tmp/'
output_dir = '../data/output/'


##retrieve changeset xls files from destatis
currentYear = datetime.now().year
#for i in range(2010, currentYear + 1):
#    changeset_xls = os.path.join(input_dir, 'changesets', str(i)+'.xls')
#    urllib.urlretrieve ('https://www.destatis.de/DE/ZahlenFakten/LaenderRegionen/Regionales/Gemeindeverzeichnis/NamensGrenzAenderung/Aktuell/'+str(i)+'.xls?__blob=publicationFile', changeset_xls)


#retrieve current ags list from destatis
current_ags_list_xls = os.path.join(input_dir, 'current_state', 'ags_list.xls')
#urllib.urlretrieve ('https://www.destatis.de/DE/ZahlenFakten/LaenderRegionen/Regionales/Gemeindeverzeichnis/Administrativ/Archiv/GVAuszugQ/AuszugGV1QAktuell.xlsx?__blob=publicationFile', current_ags_list_xls)


#current ags_list to dataframe, set column types, rename headers
df_current_ags = pd.read_excel(current_ags_list_xls, sheetname=1, header=1, skiprows=range(0, 4), 
                               converters={'Unnamed: 1':str,'Unnamed: 2':str,'Unnamed: 3':str,
                                           'Unnamed: 4':str,'Unnamed: 5':str,'Unnamed: 6':str,
                                           'Unnamed: 14':str,'Unnamed: 17':str,'Unnamed: 19':str})

header = ['satzart','textkennzeichen',
          'land','rb','kreis','vb','gemeinde',
          'name','area','area_date',
          'pop_sum','pop_m','pop_w','pop_p_km2',
          'plz','lat','lng','rg_key','rg_name','urb_key','urb_class']	
df_current_ags.columns = header


#apply filters (drop rows where gemeinde = nan) / generate current ags column / drop unused columns / export to csv
df_current_ags = df_current_ags[df_current_ags.gemeinde.notnull()]
df_current_ags['ags_new'] = df_current_ags['land'] + df_current_ags['rb'] + df_current_ags['kreis'] + df_current_ags['gemeinde']

cols = ['satzart','textkennzeichen','gemeinde',
          'land','rb','kreis','vb','area_date','rg_key','rg_name']
for col in cols:
    df_current_ags.drop(col, axis=1, inplace=True)

df_current_ags.to_csv(os.path.join(tmp_dir, 'ags_current.csv'), index=False, encoding='utf-8', sep='\t', decimal=',' )


#list input datasets, add to list
in_files = []
for file in os.listdir(os.path.join(input_dir, 'changesets')):
    if file.endswith(".xls"):
        in_files.append((file.replace('.xls',''), os.path.join(input_dir, 'changesets', file)))
     
in_files = in_files[::-1]
        
#iterate through input datasets
for file in in_files:
       
    # changeset to dataframe, set column types, rename headers
    df = pd.read_excel(file[1], sheetname=1, header=1, skiprows=range(0, 5), converters={'Unnamed: 2':str,'Unnamed: 3':str,'Unnamed: 5':str,'Unnamed: 8':str,'Unnamed: 9':str})
    
    header = ['change_id',
              'regionaleinheit',
              'rs_old',
              'ags_old',
              'name_old',
              'change_type',
              'area_m2',
              'pop',
              'rs_new',
              'ags_new',
              'name_new',
              'change_date_jur',
              'change_date_sta']	
    df.columns = header
    
    
    #apply filters
    df = df[(df['regionaleinheit']  == 'Gemeinde') & ((df['change_type'] == '1') | (df['change_type'] == '3') | (df['change_type'] == '4'))]
    
    
    #drop unused columns and reorder dataframe
    cols = ['change_id',
            'regionaleinheit',
            'change_type',
            'rs_old',
            'area_m2',
            'pop',
            'rs_new',
            'change_date_jur',
            'change_date_sta']
    
    for col in cols:
        df.drop(col, axis=1, inplace=True)
        
    cols = ['ags_new','name_new','ags_old','name_old']	
    df = df[cols]
    
    #save filtered dataframe to csv
    filename = os.path.join(tmp_dir, file[0]+'.csv')
    df.to_csv(filename, index=False, encoding='utf-8', sep='\t', decimal=',' )