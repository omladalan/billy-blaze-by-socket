import os
import websocket
import _thread
import time
import rel
from datetime import datetime
import json
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient

load_dotenv(find_dotenv())

env_mongo   = os.environ.get("MONGO")

connected = 0
cont = 0
horaexec = datetime.now()
complete = 0

def on_message(ws, message):
    global horaexec
    global complete
    #json_object = json.loads(message)
    #print(json_object.json())
    if connected > 0:
        """
        try:
            teste = message.index('[')
        except ValueError:
            print(">>",teste)   
        else:
            print("Found!")
        """

        #if message.index('[') == "substring not found":
        #    print(">kkk",message.index('['))         

        #print(len(message))
        
        if len(message) > 100:
            #print(message)
            str = message.split('"id":"')
            str = str[1].split('"')
            #print(str[0])
            id = str[0]
            if id == "double.tick":
                ini = message.index('[')
                #print(message.index('['))
                #print(message[ini:].json())
                d = json.loads(message[ini:])
                #waiting complete rolling
                #print(d[1]['payload']['id'])
                #print(d[1]['payload']['status'])
                #print(d['data'])
                #string_sliced = message.slice(2, len(message))
                #print('string_sliced', string_sliced)
                if d[1]['payload']['status'] == 'complete':
                    if complete == 0:
                        #print(d[1]['payload'])
                        #print('\n\n')

                        kkk = get_database()
                        collection_name = kkk["blaze"]
                        d[1]['payload'].pop('bets')
                        collection_name.insert_one(d[1]['payload'])

                        complete = 1
                else:
                    complete = 0
            else:
                print(id)
        now = datetime.now()
        #print('now', now)
        #print('horaexec',  horaexec)
        diff = now - horaexec  
        #print('diff', diff.total_seconds())
        if diff.total_seconds() >= 5:
            #print('send')
            ws.send('2')
            horaexec = datetime.now()

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###", close_status_code, close_msg)

def on_open(ws):
    #print(ws)
    #print(vars(ws))
    #ws.send('423["cmd",{"id":"subscribe","payload":{"room":"double"}}]')
    ws.send('423["cmd",{"id":"subscribe","payload":{"room":"double_v2"}}]')
    global connected
    connected = 1
    #print(connected)
    print("Opened connection")

def on_data(ws, ws1, ws2, ws3):
    print("Data: ", ws1, ws2, ws3)

def get_database():
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = env_mongo
   
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['test']

if __name__ == "__main__":
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp("wss://api-v2.blaze.com/replication/?EIO=3&transport=websocket",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close,
                              header     = {'Origin': 'https://blaze.com',
                                            'Upgrade': 'websocket',
                                            'Sec-Webscoket-Extensions': 'permessage-defalte; client_max_window_bits',
                                            'Pragma': 'no-cache',
                                            'Connection': 'Upgrade',
                                            'Accept-Encoding': 'gzip, deflate, br',
                                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
                                            }
                              )
    
    #print(vars(kkk.list_collection_names))
    #ws.run_forever()
    #ws.run_forever(dispatcher=rel, reconnect=5)  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly
    #rel.signal(2, rel.abort)  # Keyboard Interrupt
    #rel.dispatch()
    ws.run_forever(dispatcher=rel, reconnect=5)  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly
    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()