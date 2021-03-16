import sqlite3
import json
from datetime import datetime
import pytz

tz = pytz.timezone('Asia/Kolkata')

conn = sqlite3.connect('rasa.db')
print("Opened database successfully")

cursor = conn.execute("SELECT * from conversation_session")

sessions = {}

for row in cursor:
    sender_id = row[0]
    print("SESSION ID :: ", sender_id)
    if sender_id not in sessions:
        sessions[sender_id] = []

    try:
        convs = conn.execute("SELECT * from conversation_event WHERE conversation_id = '{0}'".format(sender_id))
        print(convs)
        
        for r in list(convs):
            sessions[sender_id].append(r)
            # print(r[0],r[1], "===+++++\n")
            # print("type_name :: ",r[2])
            # print("timestamp :: ",r[3])
            # print("intent_name :: ",r[4])
            # print("action_name :: ",r[5])
            # print("policy :: ",r[6])
            # print("is_flagged :: ",r[7])
            # print("data :: ",r[8])
            # print("evaluation :: ",r[9])
            # print("NEXT  \n")

        print("====\n")
        
    except:
        print("!!\n")
    

print("Operation done successfully")
conn.close()


def get_tracker(data):
    trackers = []
    tracker = {"transcript":[], "slots":{}, "timestamp":None}
    for i, item in enumerate(data):
        timestamp = item[3]
        dt = datetime.fromtimestamp(timestamp, tz)
        #print(dt)
        type_name = item[2]
        intent_name = item[4]
        action_name = item[5]
        meta_data = json.loads(item[8])

        if type_name == "session_started": #or (type_name == "user" and meta_data['text'] == "bot start query"):
            #tracker['timestamp'] = timestamp
            if tracker['transcript']:
                trackers.append(tracker)
                del tracker
            tracker = {"transcript":[], "slots":{}, "timestamp":timestamp}
        
        if type_name == "user" and meta_data['text'] == "bot start query":
            tracker['timestamp'] = timestamp
            
                
        

        if type_name == "slot":
            tracker['slots'][meta_data['name']] = meta_data['value']

        elif type_name == "user":
            tracker['transcript'].append({"person":"user", "text":meta_data['text'], "intent": intent_name, "timestamp":timestamp})
        
        elif type_name == "bot":
            tracker['transcript'].append({"person":"bot", "text":meta_data['text'], "timestamp":timestamp})

        # elif type_name == "action":
        #     if action_name != "action_listen":
        #         tracker['transcript'].append({"event":"action", "action_name":action_name, "timestamp":timestamp})


    
    if tracker['transcript']:
        trackers.append(tracker)

    return trackers

def get_ist_time(t):
    dt = datetime.fromtimestamp(t, tz)
    return dt.strftime('%d/%m/%Y %H:%M:%S') 


count = 0
count2 = 0


phone, call_time, transcript, slots, ist_time = [],[],[],[], []

for session, data in sessions.items():
    print("SESSION ID ::: ", session)

    if "--" in session:

        trackers = get_tracker(data)
        n = len(trackers)
        count +=1
        count2 += n

        print("TOTAL TRACKER FOUND :: ", n)
        if n > 1:
            print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

        for tracker in trackers:

            _phone = tracker['slots']['phoneNo'] if "phoneNo" in tracker['slots'] and tracker['slots']['phoneNo'] else session.split("--")[0]
            _time = tracker['timestamp']
            _transcript = json.dumps({"transcript":tracker['transcript']})
            _slots = json.dumps(tracker['slots'])
            _ist = get_ist_time(_time)

            phone.append(_phone)
            call_time.append(_time)
            transcript.append(_transcript)
            slots.append(_slots)
            ist_time.append(_ist)


            t = json.dumps(tracker)
            #print(jsonify(tracker))
            print(t)
            print("-----------------\n")

        print("+==================\n")


print(count, count2)

import pandas as pd

df = pd.DataFrame({"phone":phone, "ist_time":ist_time, "call_time":call_time, "slots":slots,"transcript":transcript})

df.to_csv("test_v2.csv")










# crt phone transcript slots time