import json
import datetime

import asyncio
import aiohttp

import ccxt.async as ccxt


try:
    from .logging_agent import logging
except:
    from logging_agent import logging


from db_utils import Database
from config import GDAX_PRODUCT_IDS, DATABASE


class DataFeed():

    asyncio_loop = None

    def __init__(self):
        self.url = "wss://ws-feed.gdax.com"
        self.public_client = ccxt.gdax()

        self.product_ids = GDAX_PRODUCT_IDS

        self.order_books = {x: {} for x in self.product_ids}
        self.inside_order_books = {
            x: {"bids": {}, "asks": {}} for x in self.product_ids
        }
        self.last_trade_ids = {x: None for x in self.product_ids}

        self.db = Database(DATABASE['GDAX'], migrate=False)
        self.migrate = True

    async def web_socket_handler(self):
        print("connecting with websocket")
        request_packet = self.get_request_packet()
        try:
            async with aiohttp.ClientSession().ws_connect(self.url) as ws:
                await ws.send_json(request_packet)
                async for msg in ws:
                    if msg.tp == aiohttp.WSMsgType.text:
                        if msg.data == 'close':
                            await ws.close()
                            await self.web_socket_handler()
                        else:
                            await self.message_builder(msg.data)
                    elif msg.tp == aiohttp.WSMsgType.closed:
                        await ws.close()
                        await self.web_socket_handler()  # reconnect with the web socket
                    elif msg.tp == aiohttp.WSMsgType.error:
                        await ws.close()
                        await self.web_socket_handler()  # reconnect with the web socket
        except aiohttp.client_exceptions.WSServerHandshakeError:
            logging.error("GDAX: WSServerHandshakeError, Reconnecting")
            asyncio.sleep(2)  # 2 sec sleep before re-connecting again
            await self.web_socket_handler()

    async def message_builder(self, msg):
        msg = json.loads(msg)
        if msg['type'] == 'snapshot':
            product_id = msg['product_id']
            self.order_books[product_id] = {'bids': msg['bids'], 'asks': msg['asks']}

        if msg['type'] == 'l2update':
            product_id = msg['product_id']
            changes = msg['changes']

            for change in changes:
                change_side = 'bids' if change[0] == 'buy' else 'asks'
                change_price = float(change[1])
                change_volume = float(change[2])

                orders = self.order_books[product_id][change_side]
                level_index = [i for i, order in enumerate(orders) if float(order[0])==float(change[1])]

                if level_index:
                    if float(change[2]) != 0:
                        self.order_books[product_id][change_side][min(level_index)][1] = change[2]
                    else:
                        self.order_books[product_id][change_side].pop(min(level_index))

                if not level_index:
                    if change_side == 'bids':
                        insert_indexes = [i for i, order in enumerate(orders) if float(order[0]) >= float(change[1])]
                    if change_side == 'asks':
                        insert_indexes = [i for i, order in enumerate(orders) if float(order[0]) <= float(change[1])]
                    if not insert_indexes:
                        insert_index = -1
                    else:
                        insert_index = max(insert_indexes)
                    self.order_books[product_id][change_side].insert(insert_index+1, [change[1],change[2]])

            inside_bids = {'bids_'+str(x+1): "@".join(self.order_books[product_id]['bids'][x][::-1]) for x in range(15)}
            inside_asks = {'asks_'+str(x+1): "@".join(self.order_books[product_id]['asks'][x][::-1]) for x in range(15)}
            inside_order_book = {"bids": inside_bids, "asks": inside_asks}

            if self.inside_order_books[product_id] != inside_order_book:
                row = {
                    "server_datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                    "product_id": product_id
                }
                row.update(inside_bids)
                row.update(inside_asks)

                await self.db.insert_into("gdax_order_book", data=row)
                self.inside_order_books[product_id] = inside_order_book
                # print(row)

        if msg['type'] == 'match':
            product_id = msg['product_id']
            trades = [{
                "server_datetime":datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                "exchange_datetime": msg['time'],
                "sequence": msg['sequence'],
                "trade_id": msg['trade_id'],
                "product_id": product_id,
                'price': msg['price'],
                'volume': msg['size'],
                'side': msg['side'],
                'backfilled': 'False'
            }]

            current_trade_id = int(msg["trade_id"])
            if self.last_trade_ids[product_id]:
                last_trade_id = int(self.last_trade_ids[product_id])
            else:
                last_trade_id = current_trade_id
            self.last_trade_ids[product_id] = msg["trade_id"]
            if current_trade_id > (last_trade_id + 1):
                missing_trade_ids = list(range(last_trade_id + 1, current_trade_id))
                print("missed the following trades: "+str(missing_trade_ids))
                after_arg = None
                recursive_count = 1
                while missing_trade_ids and recursive_count < 4:
                    ccxt_product_id = product_id.replace("-", "/")
                    params = {}
                    if after_arg:
                        params = {'after': after_arg}
                    product_trades = await self.public_client.fetch_trades(ccxt_product_id, params)
                    product_trades = [product_trade['info'] for product_trade in product_trades]
                    product_trades_dict = {}
                    for product_trade in product_trades:
                        product_trades_dict[int(product_trade['trade_id'])] = product_trade
                    for missing_trade_id in missing_trade_ids:
                        if missing_trade_id in product_trades_dict:
                            missing_product_trade = product_trades_dict[missing_trade_id]
                            missing_trade = {
                                    "server_datetime":datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"), #2017-10-15T05:10:53.700000Z
                                    "exchange_datetime":missing_product_trade['time'],
                                    "sequence":"None",
                                    "trade_id":missing_product_trade['trade_id'],
                                    "product_id": product_id,
                                    'price':missing_product_trade['price'],
                                    'volume':missing_product_trade['size'],
                                    'side':missing_product_trade['side'],
                                    'backfilled':'True'
                            }
                            trades.append(missing_trade)
                        else:
                            print("Trade missed {}".format(missing_trade_id))

                    missing_trade_ids = list(set(missing_trade_ids) - set(list(product_trades_dict.keys())))
                    if missing_trade_ids:
                        after_arg = max(missing_trade_ids) + 1
                        recursive_count = recursive_count + 1
                    final_missing_trades = (set(missing_trade_ids) - set(list(product_trades_dict.keys())))
                    trades_filled = list((set(list(product_trades_dict.keys())) - set(missing_trade_ids)))
                    logging.error("__________________________________________________________________")
                    logging.error("GDAX: Recusive Count {}".format(recursive_count))
                    logging.error("GDAX: Trades finally filled {}".format(trades_filled))
                    logging.error("GDAX: Trades finally missing {}".format(final_missing_trades))
                    logging.error("GDAX: Required Trades {}".format(missing_trade_ids))
                    logging.error("GDAX: Trades from API: {}".format(product_trades_dict.keys()))
                    logging.error("__________________________________________________________________")

            for trade in trades:
                await self.db.insert_into("gdax_trades", trade)
                print (trade)

    def get_request_packet(self):
        request_packet = {
            "type": "subscribe",
            "product_ids": self.product_ids,
            "channels": ["level2", "matches"]
            # "channels": ["matches"]
        }
        return request_packet

    async def subscribe_to_exchange(self):
        if self.migrate:
            await self.db.migrate()
        await self.web_socket_handler()


if __name__ == "__main__":
    feed = DataFeed()
    loop = asyncio.get_event_loop()
    asyncio.async(feed.subscribe_to_exchange())
    loop.run_forever()
