from os import listdir
import pandas as pd
from bson import objectid
import numpy as np

def df_load(path):
    raw_data=listdir(path)
    cleaned = []
    dfs = {}  
    
    for f in raw_data:
        name = f.removesuffix(".csv")      # remove extension
        if "_" in name:
            name = name.split("_", 1)[1]   # take everything after the first "_"
        name = name.removeprefix("_")
        name= name.removesuffix("__anonymized")
        name= name.removesuffix("_anon")

        cleaned.append(name)
        
        
        df = pd.read_csv(f'{path}/{f}')  # adjust path if needed
        
        # store in dictionary
        dfs[name] = df
    return dfs

# unpacking script
# for name, df in dfs.items():
#     globals()[name] = df
#     print(name)

def remove_columns(df):
    for col in df.columns:
        if df[col].notnull().sum()==0:
            df.drop(columns=[col],inplace=True)

    for col in df.columns:
        if df[col].nunique()==1:
            df.drop(columns=[col],inplace=True)
    return df


def get_creation_time(x):
    if pd.isna(x):  
        return pd.NaT
    try:
        return objectid.ObjectId(str(x)).generation_time.replace(tzinfo=None)
    except Exception:
        return pd.NaT
    
def get_salary(df,hours_column):
    salary_dict_base={
        'K1': 250,
        'K2': 250,
        'K3': 250,
        'K4': 250,
        np.nan: 250}
    salary_dict_per_hour={
        'K1': 0,
        'K2': 50,
        'K3': 120,
        'K4': 220,
        np.nan: 0}
    # map base salary
    df['base_salary'] = df['seniority'].map(salary_dict_base)
    # map per-hour salary
    df['per_hour_salary'] = df['seniority'].map(salary_dict_per_hour)
    df['salary'] =  df[hours_column].fillna(0) * (df['per_hour_salary']+ df['base_salary'])
    df['social_fees'] = df['salary'] * 0.38
    df['total_cost'] = df['salary'] + df['social_fees']
    return df