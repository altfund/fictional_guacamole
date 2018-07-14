import asyncio
import datetime
import json

import aiohttp

from config import POLO_PRODUCT_IDS, DATABASE
from db_utils import Database

try:
    from .logging_agent import logging
except:
    from logging_agent import logging


def sorting_key(dict_key):
    return float(dict_key)


class DataFeed():
    asyncio_loop = None

    def __init__(self):
        self.url = "wss://api2.poloniex.com"

        self.product_ids = POLO_PRODUCT_IDS
        self.product_codes = {}
        self.order_books = {x: {} for x in self.product_ids}
        self.inside_order_books = {x: {"bids": {}, "asks": {}} for x in self.product_ids}
        self.last_trade_ids = {x: None for x in self.product_ids}

        self.db = Database(DATABASE['POLO'], migrate=False)
        self.migrate = True
        self.asyncio_loop = self.asyncio_loop or asyncio.get_event_loop()
        self.aiohttp_session = aiohttp.ClientSession(loop=self.asyncio_loop)

    async def web_socket_handler(self):
        request_packets = self.get_request_packet()
        async with self.aiohttp_session.ws_connect(self.url) as ws:
            for packet in request_packets:
                await ws.send_json(packet)
            async for msg in ws:
                if msg.tp == aiohttp.WSMsgType.text:
                    if msg.data == 'close':
                        await ws.close()
                        await self.web_socket_handler()
                    else:
                        await self.message_builder(msg.data)
                elif msg.tp == aiohttp.WSMsgType.closed:
                    await self.web_socket_handler()  # reconnect with the web socket
                elif msg.tp == aiohttp.WSMsgType.error:
                    await self.web_socket_handler()  # reconnect with the web socket


    async def message_builder(self, msg):
        msg = json.loads(msg)

        try:
            for message in msg[2]:  # maybe will be better if current implementation doesn't work
                if message[0] == 'i':
                    print("got ob snapshot")
                    all_bids = message[1]['orderBook'][1]
                    all_asks = message[1]['orderBook'][0]
                    final_all_bids = [[x, all_bids[x]] for x in sorted(all_bids, key=sorting_key, reverse=True)]
                    final_all_asks = [[x, all_asks[x]] for x in sorted(all_asks, key=sorting_key)]
                    self.order_books[message[1]['currencyPair']] = {
                        'bids': final_all_bids,
                        'asks': final_all_asks}
                    self.product_codes[msg[0]] = message[1]['currencyPair']
                elif message[0] == 'o':
                    change_side = 'bids' if message[1] == 1 else 'asks'
                    orders = self.order_books[self.product_codes[msg[0]]][change_side]
                    level_index = [i for i, order in enumerate(orders) if float(order[0]) == float(message[2])]
                    if level_index:
                        if float(message[3]) != 0:
                            self.order_books[self.product_codes[msg[0]]][change_side][min(level_index)][1] = message[3]
                        else:
                            self.order_books[self.product_codes[msg[0]]][change_side].pop(min(level_index))
                    if not level_index:
                        insert_indexes = None
                        if change_side == 'bids':
                            insert_indexes = [i for i, order in enumerate(orders) if float(order[0]) >= float(message[2])]
                        if change_side == 'asks':
                            insert_indexes = [i for i, order in enumerate(orders) if float(order[0]) <= float(message[2])]
                        if not insert_indexes:
                            insert_index = -1
                        else:
                            insert_index = max(insert_indexes)
                        self.order_books[self.product_codes[msg[0]]][change_side].insert(insert_index + 1,
                                                                                         [message[2], message[3]])

                    inside_bids = {
                    'bids_' + str(x + 1): "@".join(self.order_books[self.product_codes[msg[0]]]['bids'][x][::-1]) for x in
                    range(15)}
                    inside_asks = {
                    'asks_' + str(x + 1): "@".join(self.order_books[self.product_codes[msg[0]]]['asks'][x][::-1]) for x in
                    range(15)}
                    inside_order_book = {"bids": inside_bids, "asks": inside_asks}

                    if self.inside_order_books[self.product_codes[msg[0]]] != inside_order_book:
                        row = {
                            "server_datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                            "product_id": self.product_codes[msg[0]]
                        }
                        row.update(inside_bids)
                        row.update(inside_asks)

                        await self.db.insert_into("polo_order_book", row)

                        self.inside_order_books[self.product_codes[msg[0]]] = inside_order_book
                        print(row)

                elif message[0] == 't':
                    # TRADES
                    # ["t","9394200",1,"5545.00000000","0.00009541",1508060546]
                    # [trade, tradeId, 0/1 (sell/buy), price, amount, timestamp]
                    # print(message)
                    trades = [{
                        "server_datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                        "exchange_datetime": datetime.datetime.fromtimestamp(message[5]).strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                        "sequence": msg[1],
                        "trade_id": message[1],
                        "product_id": self.product_codes[msg[0]],
                        'price': message[3],
                        'volume': message[4],
                        'side': 'sell' if message[2] == 0 else 'buy',
                        'backfilled': 'False'
                    }]

                    current_trade_id = int(message[1])
                    if self.last_trade_ids[self.product_codes[msg[0]]]:
                        last_trade_id = int(self.last_trade_ids[self.product_codes[msg[0]]])
                    else:
                        last_trade_id = current_trade_id
                    self.last_trade_ids[self.product_codes[msg[0]]] = message[1]
                    if current_trade_id > (last_trade_id + 1):
                        missing_trade_ids = list(range(last_trade_id + 1, current_trade_id))
                        print("missed the following trades: " + str(missing_trade_ids))

                    for trade in trades:
                        await self.db.insert_into("polo_trades", trade)
                        print(trade)
        except IndexError:
            logging.error("Poloniex: Invalid Message {}".format(msg))


    def get_request_packet(self):
        packet_list = []
        for x in self.product_ids:
            packet_list.append(({'command': 'subscribe', 'channel': x}))
        return packet_list

    async def subscribe_to_exchange(self):
        if self.migrate:
            await self.db.migrate()
        await self.web_socket_handler()


if __name__ == "__main__":
    feed = DataFeed()
    loop = asyncio.get_event_loop()
    asyncio.async(feed.subscribe_to_exchange())
    loop.run_forever()
