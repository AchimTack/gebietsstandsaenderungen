# -*- coding: utf-8 -*-
"""
Created on Sun Oct 01 09:44:45 2017

@author: Achim Tack
"""

#imports
import pandas as pd
import os


#paths
input_dir = '../data/input/'
output_dir = '../data/output/'


#list input datasets
in_files = []
for file in os.listdir(input_dir):
    if file.endswith(".xls"):
        in_files.append((file.replace('.xls',''), os.path.join(input_dir, file)))


#iterate through input datasets
for file in in_files:
    # excel to dataframe, set column types, rename headers
    df = pd.read_excel(file[1], sheetname=1, header=1, skiprows=range(0, 5), converters={'Unnamed: 2':str,'Unnamed: 3':str,'Unnamed: 5':str,'Unnamed: 8':str,'Unnamed: 9':str})
    
    header = ['change_id','regionaleinheit','regionalschlüssel_old',
    'ags_old','name_old','change_type',
    'area_m2','pop','regionalschlüssel_new',
    'ags_new','name_new','change_date_jur','change_date_stat']	
    
    df.columns = header
    
    
    #apply filters
    df = df[(df['regionaleinheit']  == 'Gemeinde') & ((df['change_type'] == '1') | (df['change_type'] == '3') | (df['change_type'] == '4'))]
    
    
    #drop unused columns and reorder dataframe
    cols = ['regionaleinheit','regionalschlüssel_old','area_m2','pop','regionalschlüssel_new','change_date_jur']
    for col in cols:
        df.drop(col, axis=1, inplace=True)
        
    cols = ['change_id','change_type','ags_new','name_new','ags_old','name_old', 'change_date_stat']
    df = df[cols]
    
    #save filtered dataframe to csv
    filename = os.path.join(output_dir, file[0]+'.csv')
    df.to_csv(filename, index=False, encoding='utf-8', sep='\t', decimal=',' )



		
