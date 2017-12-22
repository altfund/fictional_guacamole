import os
import sys
import json
import datetime
import websocket

from db_utils import Database
from config import POLO_PRODUCT_IDS, DATABASE


class DataFeed():

    def __init__(self):
        self.url = "wss://api2.poloniex.com"
        #self.public_client = polo.PublicClient()
        
        self.product_ids = POLO_PRODUCT_IDS
        self.product_codes = {}
        self.order_books = {x:{} for x in self.product_ids}
        self.inside_order_books = {x:{"bids":{},"asks":{}} for x in self.product_ids}
        self.last_trade_ids = {x:None for x in self.product_ids}
        
        self.db = Database(DATABASE['POLO'], migrate=True)

        self.ws = websocket.WebSocketApp(
            self.url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_open=self.on_open,
        )

    def on_message(self,ws,msg):
        msg = json.loads(msg)

        for message in msg[2]: #maybe will be better if current implementation doesn't work
            if message[0] == 'i':
                print("got ob snapshot")
                self.order_books[message[1]['currencyPair']] = {'bids':[[x, message[1]['orderBook'][1][x]] for x in message[1]['orderBook'][1]],'asks':[[x, message[1]['orderBook'][0][x]] for x in message[1]['orderBook'][0]]}
                self.product_codes[msg[0]] = message[1]['currencyPair']
            elif message[0] == 'o':
                change_side = 'bids' if message[1]==1 else 'asks'
                orders = self.order_books[self.product_codes[msg[0]]][change_side]
                level_index = [i for i, order in enumerate(orders) if float(order[0])==float(message[2])]
                if level_index:
                    if float(message[3]) != 0:
                        self.order_books[self.product_codes[msg[0]]][change_side][min(level_index)][1] = message[3]
                    else:
                        self.order_books[self.product_codes[msg[0]]][change_side].pop(min(level_index))
                if not level_index:
                    if change_side == 'bids':
                        insert_indexes = [i for i, order in enumerate(orders) if float(order[0]) >= float(message[1])]
                    if change_side == 'asks':
                        insert_indexes = [i for i, order in enumerate(orders) if float(order[0]) <= float(message[1])]
                    if not insert_indexes:
                        insert_index = -1
                    else:
                        insert_index = max(insert_indexes)
                    self.order_books[self.product_codes[msg[0]]][change_side].insert(insert_index+1, [message[2],message[3]])
                        
                inside_bids = {'bids_'+str(x+1):"@".join(self.order_books[self.product_codes[msg[0]]]['bids'][x][::-1]) for x in range(15)}
                inside_asks = {'asks_'+str(x+1):"@".join(self.order_books[self.product_codes[msg[0]]]['asks'][x][::-1]) for x in range(15)}
                inside_order_book = {"bids":inside_bids,"asks":inside_asks}
                
                if self.inside_order_books[self.product_codes[msg[0]]] != inside_order_book:
                    row = {
                        "server_datetime":datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                        "product_id":self.product_codes[msg[0]]
                    }
                    row.update(inside_bids)
                    row.update(inside_asks)

                    sql = INSERT_SQL.format(
                        table="polo_order_book",
                        fields=",".join(ORDER_FIELDS), 
                        values=",".join([":{}".format(field) for field in ORDER_FIELDS]),
                    )
                    self.db.insert_into("polo_order_book", row)

                    self.inside_order_books[self.product_codes[msg[0]]] = inside_order_book
                    print(row)
                
                            
            elif message[0] == 't':
                # TRADES
                # ["t","9394200",1,"5545.00000000","0.00009541",1508060546]
                # [trade, tradeId, 0/1 (sell/buy), price, amount, timestamp]
                #print(message)
                trades = [{
                    "server_datetime":datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                    "exchange_datetime":datetime.datetime.fromtimestamp(message[5]).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                    "sequence":msg[1],
                    "trade_id":message[1],
                    "product_id":self.product_codes[msg[0]],
                    'price':message[3],
                    'volume':message[4],
                    'side':'sell' if message[2]==0 else 'buy',
                    'backfilled':'False'
                }]
                
                current_trade_id = int(message[1])
                if self.last_trade_ids[self.product_codes[msg[0]]]:
                    last_trade_id = int(self.last_trade_ids[self.product_codes[msg[0]]])
                else:
                    last_trade_id = current_trade_id
                self.last_trade_ids[self.product_codes[msg[0]]] = message[1]
                if current_trade_id > (last_trade_id + 1):
                    missing_trade_ids = list(range(last_trade_id + 1, current_trade_id))
                    print("missed the following trades: "+str(missing_trade_ids))
                    
                for trade in trades:
                    self.db.insert_into("polo_trades", trade)
                    print(trade)

    def on_error(self,ws,error):
        print(error)
        
    def on_open(self,ws):
        #request = {
        #    "type": "subscribe",
        #    "channel": "BTC_ETH"}
        #request = json.dumps(request)
        #request = request.encode("utf-8")
        for x in self.product_ids:
            ws.send(json.dumps({'command':'subscribe','channel': x}))
        #ws.send(request)
    
    def run(self):
        try:
            self.ws.run_forever()
        except KeyboardInterrupt:
            sys.exit()
        except Exception:
            pass

if __name__ == "__main__":
    feed = DataFeed()
    feed.run()
    
# https://stackoverflow.com/questions/32154121/how-to-connect-to-poloniex-com-websocket-api-using-a-python-library

#1001 = trollbox (you will get nothing but a heartbeat)
#1002 = ticker
#1003 = base coin 24h volume stats
#1010 = heartbeat
#'MARKET_PAIR' = market order books

# ORDERBOOK SNAPSHOT
# [114,247989292,[["i",{"currencyPair":"BTC_XMR","orderBook":[{"0.01629561":"2.32977526","0.01629618":"14.77417529",
# orderbook[0] = asks, orderbook[1] = bids (inferred from pricing: 0 is increasing price from middle, 1 is decreasing price from middle)

# ORDERS
#  [148,394056638,[["o",0,"0.07615527","0.34317849"]]]
# 148: appears to be the ID of the Currency Pair.
# 394056638: Sequence number (I presume of the last sequence value prior to the full orderbook as presented) 
# "o" + 0: orderBookRemove "o" + 1: orderBookModify 
# "0.07615527": Price 
# "0.34317849": Quantity

# TRADES
# ["t","9394200",1,"5545.00000000","0.00009541",1508060546]
# [trade, tradeId, 0/1 (sell/buy), price, amount, timestamp]