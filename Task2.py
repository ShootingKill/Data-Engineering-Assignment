from __future__ import absolute_import
from __future__ import print_function
import os
import json
from kafka import KafkaConsumer
import time as sleeptime
from datetime import datetime as dating 
import happybase as hb



def insert_row(table, row):
    try:
        
        row_id = '{}'.format([row['id'],row['date'],row['time']]) 
        date = '{}'.format(row['date']) 
        time = '{}'.format(row['time']) 
        hour = '{}'.format(row['hour']) 
        day = '{}'.format(row['day']) 
        tweet = '{}'.format(row['tweet'])
        hashtags = '{}'.format(row['time']) 
        video = '{}'.format(row['video']) 
        nlikes = '{}'.format(row['nlikes']) 
        nreplies = '{}'.format(row['nreplies']) 
        nretweets = '{}'.format(row['nretweets']) 
        tokenized = '{}'.format(row['tokenized']) 
        
        table.put(row_id, {'metadata:hashtags': hashtags, 'video:video': video, 'time_dimension:date': date,
                           'time_dimension:day': day, 'time_dimension:hour': hour, 'time_dimension:time': time, 
                           'numcount:nlikes': nlikes, 'numcount:nreplies': nreplies, 'numcount:nretweets': nretweets,
                           'text:tweet':tweet, 'text:tokenized':tokenized}) 
        
        print('Insert data: {},[{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}]'.format(row_id, date,time,hour,day,tweet,
                                                                                    hashtags,video,nlikes,nreplies,nretweets,
                                                                                    tokenized))
        raise ValueError("Something went wrong!")
    except ValueError as e:
        # error handling goes here; nothing is sent to HBase
        pass
    else:
        # no exceptions; send data
        b.send()                                                                   
                                                                                              
connection = hb.Connection('sandbox-hdp.hortonworks.com', autoconnect=True, port=9097,transport="buffered")             
connection.open()

if b'english' not in connection.tables():
    
    connection.create_table(                                                                      
        'english',                                                                                
        {'metadata': dict(),                                                                      
         'text': dict(),                                                                          
         'time_dimension': dict(),                                                                
         'video': dict(),                                                                         
         'numcount': dict()                                                                       
        }                                                                                         
    )   

if b'malay' not in connection.tables():
    
    connection.create_table(                                                                      
        'malay',                                                                                
        {'metadata': dict(),                                                                      
         'text': dict(),                                                                          
         'time_dimension': dict(),                                                                
         'video': dict(),                                                                         
         'numcount': dict()                                                                       
        }                                                                                         
    )    

if b'chineseNew' not in connection.tables():
    
    connection.create_table(                                                                      
        'chineseNew',                                                                                
        {'metadata': dict(),                                                                      
         'text': dict(),                                                                          
         'time_dimension': dict(),                                                                
         'video': dict(),                                                                         
         'numcount': dict()                                                                       
        }                                                                                         
    )   

brokers='sandbox-hdp.hortonworks.com:6667'
sleep_time=300
offset='earliest'

consumer = KafkaConsumer(bootstrap_servers=brokers, auto_offset_reset=offset,consumer_timeout_ms=100,
                         group_id = 'LanguageReader')
consumer.subscribe(['english','chinese','malay'])


count = 0
while(True):
    for message in consumer:
        print(message)
        print(" ")
        data=json.loads(message.value)

                    
        id = data[0]
        date = data[1]
        time = data[2]
        hour = data[3]
        day = data[4]
        tweet = data[5]
        hashtags = data[6]
        video = data[7]
        nlikes = data[8]
        nreplies = data[9]
        nretweets = data[10]
        tokenized = data[11]
            
        data = {'id': id, 'date': date, 'time' : time, 'hour' : hour, 'day' : day, 'tweet' : tweet, 'hashtags' : hashtags,
                    'video' : video, 'nlikes' : nlikes, 'nreplies' : nreplies, 'nretweets' : nretweets, 'tokenized' : tokenized}

        if message.topic == "english" :
        
            table = connection.table("english")
            insert_row(table,data)
            
        if message.topic == "chinese" :
            table = connection.table("chineseNew")
            insert_row(table,data)
        
        if message.topic == "malay" :
            table = connection.table("malay")
            insert_row(table,data)

        print("-----------------------------------------")
        print(" ")
        count = count + 1
    print("datetime: ", dating.now(),"row count: ",count)
    sleeptime.sleep(5)
