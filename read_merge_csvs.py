import pandas as pd

from datetime import datetime
import pytz

tz = pytz.timezone('Asia/Kolkata')

def get_ist_time(t):
    dt = datetime.fromtimestamp(t, tz)
    return dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')


filename = "voicebot.csv"

df = pd.read_csv(filename)
print(df.head())

print(df.columns)

df["transcript"] = ""
df["slots"] = ""

useful_cols = ["Call Time", "Phone"]

file2 = "test_v2.csv"
df2 = pd.read_csv(file2)

print(df2.head())

#rslt_df = dataframe.loc[dataframe['Percentage'] > 80]

unique_nos = df.Phone.unique()
unique_nums = df2.phone.unique()

print(len(unique_nos), len(unique_nums))

cols = list(df.columns)

for num in unique_nos:
    small = df.loc[df['Phone'] == num]
    small2 = df2.loc[df2['phone'] == num]
    print(len(small), len(small2))

    if len(small) >= 1 and len(small2) >= 1 and len(small2) == len(small):
        small.sort_values(by="Call Time")
        small2.sort_values(by="ist_time")
        # print(list(small['Call Time']))
        # print(list(small2['ist_time']))
        # print("\n===============")

        transcript = list(small2['transcript'])
        slots = list(small2['slots'])
        #print(transcript[0])

        count = 0

        for index, row in small.iterrows():
            print("####################################")
            print("INDEX :: ", index)
            print (row['Phone'], df.loc[index, "Phone"], row['Phone']==df.loc[index, "Phone"])
            print(row['Call Time'], df.loc[index, "Call Time"], row['Call Time']==df.loc[index, "Call Time"])
            print (row['Phone'], df.loc[index, "Phone"], row['Phone']==df.loc[index, "Phone"])
            #row['transcript'] = transcript[count]
            #row['slots'] = slots[count]
            # df.loc[row_indexer,column_indexer]

            #print(df.loc[index,cols.index("Phone")], df.loc[index,cols.index("Call Time")])
            
            df.at[index, "transcript"] = transcript[count]
            df.at[index, "slots"] = slots[count]
            #print(df.loc[df['Phone'] == num and df['Call Time'] == row['Call Time']])#['transcript']=transcript[count]
            count +=1



df.to_csv("final_v6.csv", index=False, sep=",")


