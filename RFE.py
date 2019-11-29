import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#train = pd.read_csv('data/data_format1/train_format1.csv')
#test = pd.read_csv('data/data_format1/test_format1.csv')
#user_info = pd.read_csv('data/data_format1/user_info_format1.csv')
user_log = pd.read_csv('data/data_format1/user_log_format1.csv')
print("load success")

### GETTING COMBINED KEYS

keys = user_log[["user_id", "item_id"]]
keys.drop_duplicates(subset=None, keep = 'first',inplace=True)
keys.sort_values(["user_id", "item_id"], inplace = True)


# Calculating Recency 

recency = user_log['time_stamp'].max() - user_log['time_stamp']
df_recency = pd.DataFrame(keys)
df_recency = df_recency.join(recency)
df_recency.drop_duplicates(subset=None, keep = 'first',inplace=True)
df_recency.sort_values(["user_id", "item_id"], inplace = True)
print("Recency Calculated")

# Calculating Frequency

frequency = pd.DataFrame(keys)
frequency_count = user_log[['user_id', 'item_id', 'action_type']].groupby(['user_id', 'item_id']).count()

frequency = frequency.join(frequency_count.set_index(frequency.index),lsuffix='1')
frequency.drop_duplicates(subset=None, keep= 'first', inplace=True)

df_freq = pd.DataFrame({"user_id": keys['user_id'], "item_id": keys['item_id'], "frequency": frequency['action_type']})
print("Frequency Calculated")

# Calculating Engagement

engag = pd.DataFrame({"user_id": user_log["user_id"], "item_id": user_log["item_id"], "engagement": user_log["action_type"]},\
      columns = ["user_id", "item_id", "engagement"])
engag_sum = (engag.groupby(['user_id', 'item_id']).sum())
df_engage = pd.DataFrame({"user_id": keys["user_id"], "item_id": keys["item_id"], "engagement": engag_sum["engagement"].values},\
      columns = ["user_id", "item_id", "engagement"])
print("Engagement Calculated")      
      
def f(row):
    if row[column] <= bins[0]:
        val = 1
    elif row[column] > bins[0] and row[column] <= bins[1]:
        val = 2
    elif row[column] > bins[1] and row[column] <= bins[2]:
        val = 3
    elif row[column] > bins[2] and row[column] <= bins[3]:
        val = 4
    else:
        val = 5
    return val


# Calculating R

MinR = df_recency['time_stamp'].min() 
MaxR = df_recency['time_stamp'].max()
valR = (MaxR-MinR)/4
bins = [MinR, MinR+valR, MinR+2*valR, MaxR-valR, MaxR]
column = 'time_stamp'
df_recency['R'] = df_recency.apply(f,axis=1)
df_recency.sort_values(['time_stamp'],inplace=True)
print("R Calculated")

# Calculating F

MinF = df_freq['frequency'].min() 
MaxF = df_freq['frequency'].max()
valF = (MaxF-MinF)/4
bins = [MinF, MinF+valF, MinF+2*valF, MaxF-valF, MaxF]
column = 'frequency'
df_freq['F'] = df_freq.apply(f, axis=1)
df_freq.sort_values(['frequency'],inplace=True)
print("F Calculated")

# Calculating E

MinE = df_engage['engagement'].min() 
MaxE = df_engage['engagement'].max()
valE = (MaxE-MinE)/4
bins = [MinE, MinE+valE, MinE+2*valE, MaxE-valE, MaxE]
column = 'engagement'
df_engage['E'] = df_engage.apply(f, axis=1)
df_engage.sort_values(['engagement'],inplace=True)
print("E Calculated")

result = pd.merge(df_recency,df_freq, on = ['user_id','item_id'])
results = pd.merge(result, df_engage, on = ['user_id','item_id'])

#d = pd.concat([results["R"], results["F"], results["E"]])
def join_rfm(x): return str(x['R']) + str(x['F']) + str(x['E'])
results['RFM_Segment_Concat'] = results[['R','F', 'E']].apply(join_rfm, axis=1)

def join_rfm(x): return (x['R']) + (x['F']) + (x['E'])
results['RFM_Segment_Sum'] = results[['R','F', 'E']].apply(join_rfm, axis=1)

results.sort_values('RFM_Segment_Concat',inplace=True)
print(results)
