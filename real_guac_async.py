import asyncio
import datetime
import json

import aiohttp
import ccxt.async as ccxt

try:
    from .logging_agent import logging
except:
    from logging_agent import logging

from config import GDAX_PRODUCT_IDS
from redis_worker import DBWorker, BackFillTrades


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
        self.db_worker = DBWorker()
        self.trades_backfiller = BackFillTrades()

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

                await self.db_worker.insert_into_db(row, "gdax_order_book", exchange_name="GDAX")
                self.inside_order_books[product_id] = inside_order_book

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
                ccxt_product_id = product_id.replace("-", "/")
                # push task into the worker
                await self.trades_backfiller.backfill_trades(last_trade_id, current_trade_id, ccxt_product_id, product_id)

            for trade in trades:
                await self.db_worker.insert_into_db(trade, "gdax_trades", exchange_name="GDAX")
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
        await self.web_socket_handler()


if __name__ == "__main__":
    feed = DataFeed()
    loop = asyncio.get_event_loop()
    asyncio.async(feed.subscribe_to_exchange())
    loop.run_forever()
