import pandas as pd
import math 

def process_percentage_progression(dfa):
    df = sum_per_timestamp(dfa)
    indexing = indexing_100percent(df.shape[0])
    progression = []
    
    for index in indexing:
        percent = df.iloc[index]
        percent = percent.max().to_frame().T
        progression.append(percent)
    
    df = pd.concat(progression, ignore_index=True)
    return df

def sum_per_timestamp(df):
    #groupby sum it all 
    df = df.groupby(['timestamp']).sum()
    df = df.reset_index()
    return(df)

def indexing_100percent(length:int):
    print("length of the dataframe")
    print(length)
    interval = int(math.floor(length/100))
    print("interval of indexing")
    print(interval)
    index = []
    for i in range(100):
        lower_i = i*interval
        upper_i = (i+1) *interval
        if(upper_i > length):
            upper_i = length
        index.append(list(range(lower_i, upper_i )))
    print("indexing")
    print(index)
    return index