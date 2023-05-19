import warnings
import pandas as pd
import csv
import locale
#import bitmath
from humanfriendly import format_size, parse_size

container_name = ['webapp','worker','broker','webodm-node-odm-1','db']

def parse_docker_stats_to_pandas(filename:str):
    locale.setlocale(locale.LC_ALL, 'id_ID.utf8')
    timestamp = None
    data = []
    for line in read_large_file(filename):
        splitted = line.split()
        if(len(splitted)==0):
            #print("empty line")
            pass
        elif(len(splitted)==1):
            #skip if not valid timestamp
            if(not splitted[0].endswith(':')):
                continue
            #print("time")
            timestamp = splitted[0][:-1]
        elif(splitted[0]=="CONTAINER"):
            #print("header")
            pass
        elif(len(splitted)==14 
             and len(splitted[0]) == 12
             and splitted[1] in container_name
             and not splitted[3] == 'MEM'
             ):
            #print("data")
            container_id = splitted[0]
            name = splitted[1]
            cpu_percent = splitted[2][:-1]
            mem = splitted[3]
            mem_percent = splitted[6][:-1]

            #don't input any data without timestamp
            if(timestamp==None):
                continue
            else:
                data.append([ timestamp, container_id, name, cpu_percent, mem, mem_percent ])
        else : 
            #print(line)
            warnings.warn("this line doesn't conform to template")

    #create dataframe
    df = pd.DataFrame(data,columns=['timestamp','container_id','name','cpu_percent','mem','mem_percent'])
    #turn string timestamp to datetime
    df['timestamp'] = pd.to_datetime(df.timestamp ,format='%d_%b_%Y_%H_%M_%S')
    #turn cpu percent and mem percent to numeric
    df['cpu_percent'] = pd.to_numeric(df.cpu_percent, errors='coerce')
    df['mem_percent'] = pd.to_numeric(df.mem_percent, errors='coerce')
    #drop invalid 
    df = df.dropna()

    #turn string memory to bytes
    df['mem'] = df['mem'].apply(human2bytes)

    #return(df)
    #if there are multiple, choose the max value
    b = df.groupby(['timestamp','name']).max()
    #drop row if the name is not all exist
    mask=b.unstack().isna().any(1)  
    c=b.loc[~b.index.get_level_values(0).isin(mask[mask].index)]
    
    df = c.reset_index()

    return(df)

def parse_docker_pssize_to_pandas(filename:str):
    locale.setlocale(locale.LC_ALL, 'id_ID.utf8')
    timestamp = None
    data = []
    for line in read_large_file(filename):
        splitted = line.split()
        if(len(splitted)==0):
            #print("empty line")
            pass
        elif(len(splitted)==1):
            #skip if not valid timestamp
            if(not splitted[0].endswith(':')):
                continue
            #print("time")
            timestamp = splitted[0][:-1]
        elif(splitted[0]=="CONTAINER"):
            #print("header")
            pass
        elif(len(splitted)==5 
             and splitted[-1].endswith(')')
             ):
            container_id = splitted[0]
            name = splitted[1]
            size = splitted[2]
            size_virtual = splitted[4][:-1]

            #don't input any data without timestamp
            if(timestamp==None):
                continue
            else:
                data.append([timestamp, container_id, name, size, size_virtual])

    df = pd.DataFrame(data,columns=['timestamp','container_id','name','size','size_virtual'])
    
    df['timestamp'] = pd.to_datetime(df.timestamp ,format='%d_%b_%Y_%H_%M_%S')

    df['size'] = df['size'].apply(human2bytes)
    df['size_virtual'] = df['size_virtual'].apply(human2bytes)

    #return(df)
    #if there are multiple, choose the max value
    b = df.groupby(['timestamp','name']).max()
    # reset it 
    c = b.reset_index()

    df = c

    return(df)

def parse_nvidiasmi_to_pandas(filename:str):
    locale.setlocale(locale.LC_ALL, 'id_ID.utf8')
    timestamp = None
    data = []
    for line in read_large_file(filename):
        splitted = line.split()
        if(len(splitted)==0):
            #print("empty line")
            pass
        elif(len(splitted)==1):
            #skip if not valid timestamp
            if(not splitted[0].endswith(':')):
                continue
            #print("time")
            timestamp = splitted[0][:-1]
        elif(splitted[0]=="#"):
            #print("header")
            pass
        elif(len(splitted)>=9):
            pid = splitted[1]
            g_type = splitted[2]
            sm_percent = splitted[3]
            mem_percent = splitted[4]

            #don't input any data without timestamp
            if(timestamp==None):
                continue
            else:
                data.append([timestamp, pid, g_type, sm_percent, mem_percent])
    
    df = pd.DataFrame(data, columns=['timestamp','pid','g_type','sm_percent','mem_percent'])
    
    df['timestamp'] = pd.to_datetime(df.timestamp ,format='%d_%b_%Y_%H_%M_%S')

    #select console/non-graphical processes only
    df = df.loc[df.g_type == "C"]
    df.reset_index(inplace=True)

    #replace '-' with 0
    df['sm_percent'].replace(['-'],'0',inplace=True)
    df['mem_percent'].replace(['-'],'0',inplace=True)

    #convert to number
    df['sm_percent'] = pd.to_numeric(df.sm_percent)
    df['mem_percent'] = pd.to_numeric(df.mem_percent)

    return df

def human2bytes(hum:str):
    return parse_size(hum)
    #return(bitmath.parse_string(hum))

def read_large_file(filename):
    with open(filename, "r") as f:
        for line in f:
            yield line

if __name__ == "__main__":
    pass
    #dockerstats = parse_docker_stats_to_pandas("data/dockerstats.txt")
    #print(dockerstats)
    #dockerpssize = parse_docker_pssize_to_pandas("data/dockerpssize.txt")
    #print(dockerpssize)
    #nvidiasmi = parse_nvidiasmi_to_pandas("data/nvidiasmi.txt")
    #print(nvidiasmi)