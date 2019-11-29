import pandas as pd

class RFE:
    def __init__(self, user_log:pd.DataFrame):
        self.user_log = user_log
        self.keys = self._get_keys()
        self.results = []

    def _get_keys(self):
        ### GETTING COMBINED KEYS
        keys = self.user_log[["user_id", "item_id"]]
        keys.drop_duplicates(subset=None, keep = 'first',inplace=True)
        keys.sort_values(["user_id", "item_id"], inplace = True)

    def _f(self, row):
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

    def get_R(self):
        # Calculating Recency 
        recency = self.user_log['time_stamp'].max() - self.user_log['time_stamp']
        df_recency = pd.DataFrame(self.keys)
        df_recency = df_recency.join(recency)
        df_recency.drop_duplicates(subset=None, keep = 'first',inplace=True)
        df_recency.sort_values(["user_id", "item_id"], inplace = True)
        print("Recency Calculated")

        # Calculating R
        MinR = df_recency['time_stamp'].min() 
        MaxR = df_recency['time_stamp'].max()
        valR = (MaxR-MinR)/4
        bins = [MinR, MinR+valR, MinR+2*valR, MaxR-valR, MaxR]
        column = 'time_stamp'
        df_recency['R'] = df_recency.apply(self._f,axis=1)
        df_recency.sort_values(['time_stamp'],inplace=True)
        print("R Calculated")
        return df_recency

    def get_F(self):
        # Calculating Frequency
        frequency = pd.DataFrame(self.keys)
        frequency_count = self.user_log[['user_id', 'item_id', 'action_type']].groupby(['user_id', 'item_id']).count()
        frequency = frequency.join(frequency_count.set_index(frequency.index),lsuffix='1')
        frequency.drop_duplicates(subset=None, keep= 'first', inplace=True)
        df_freq = pd.DataFrame({"user_id": self.keys['user_id'], "item_id": self.keys['item_id'], "frequency": frequency['action_type']})
        print("Frequency Calculated")

        # Calculating F
        MinF = df_freq['frequency'].min() 
        MaxF = df_freq['frequency'].max()
        valF = (MaxF-MinF)/4
        bins = [MinF, MinF+valF, MinF+2*valF, MaxF-valF, MaxF]
        column = 'frequency'
        df_freq['F'] = df_freq.apply(self._f, axis=1)
        df_freq.sort_values(['frequency'],inplace=True)
        print("F Calculated")
        return df_freq

    def get_E(self):
        # Calculating Engagement

        engag = pd.DataFrame({"user_id": self.user_log["user_id"], "item_id": self.user_log["item_id"], "engagement": self.user_log["action_type"]},\
            columns = ["user_id", "item_id", "engagement"])
        engag_sum = (engag.groupby(['user_id', 'item_id']).sum())
        df_engage = pd.DataFrame({"user_id": self.keys["user_id"], "item_id": self.keys["item_id"], "engagement": engag_sum["engagement"].values},\
            columns = ["user_id", "item_id", "engagement"])
        print("Engagement Calculated")      
      
        # Calculating E

        MinE = df_engage['engagement'].min() 
        MaxE = df_engage['engagement'].max()
        valE = (MaxE-MinE)/4
        bins = [MinE, MinE+valE, MinE+2*valE, MaxE-valE, MaxE]
        column = 'engagement'
        df_engage['E'] = df_engage.apply(self._f, axis=1)
        df_engage.sort_values(['engagement'],inplace=True)
        print("E Calculated")
        return df_engage

    def run(self):
        df_recency = self.get_R()
        df_freq = self.get_F()
        df_engage = self.get_E()
        result = pd.merge(df_recency,df_freq, on = ['user_id','item_id'])
        results = pd.merge(result, df_engage, on = ['user_id','item_id'])

        #d = pd.concat([results["R"], results["F"], results["E"]])
        def join_rfm(x): return str(x['R']) + str(x['F']) + str(x['E'])
        results['RFM_Segment_Concat'] = results[['R','F', 'E']].apply(join_rfm, axis=1)

        def join_rfm(x): return (x['R']) + (x['F']) + (x['E'])
        results['RFM_Segment_Sum'] = results[['R','F', 'E']].apply(join_rfm, axis=1)

        results.sort_values('RFM_Segment_Concat',inplace=True)
        print(results)
        self.results = results
        return results

print("Start")
user_log = pd.read_csv('data/data_format1/user_log_format1.csv')
print("Load success")
ref = RFE(user_log)
result = ref.run()
print(result)

