from lib.parser import parse_docker_stats_to_pandas as p_ds
from lib.parser import parse_docker_pssize_to_pandas as p_dps
from lib.parser import parse_nvidiasmi_to_pandas as p_nv
from lib.filter import between

#parse the data
print("parse the data from txt")

print("dockerstats.txt")
dockerstats = p_ds("data/dockerstats.txt")
print(dockerstats)

print("dockerpssize.txt")
dockerpssize = p_dps("data/dockerpssize.txt")
print(dockerpssize)

print("nvidiasmi.txt")
nvidiasmi = p_nv("data/nvidiasmi.txt")
print(nvidiasmi)


#select the data
print("selecting data based on time between")

timetable = {
    'topdown60m1of4-ortho-5': {'start':'2023-05-09 17:15:42','end':'2023-05-09 17:26:02'},
    'topdown60m1of4-ortho-3.5': {'start':'2023-05-09 16:48:51','end':'2023-05-09 16:59:05'}
}

dockerstats_experiment = {}
dockerpssize_experiment = {}
nvidiasmi_experiment = {}
for title in timetable.keys():
    dockerstats_experiment[title] = between(
                                            timetable[title]['start'],
                                            timetable[title]['end'],
                                            dockerstats
                                            )
    dockerpssize_experiment[title] = between(
                                            timetable[title]['start'],
                                            timetable[title]['end'],
                                            dockerpssize
                                            )
    nvidiasmi_experiment[title] = between(
                                            timetable[title]['start'],
                                            timetable[title]['end'],
                                            nvidiasmi
                                            )
    
print(dockerstats_experiment)