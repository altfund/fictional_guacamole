import ccxt.async as ccxt
import math
import datetime
import asyncio

from db_utils import Database
from config import DATABASE

from arq import Actor, BaseWorker, concurrent

try:
    from .logging_agent import logging
except:
    from logging_agent import logging


class DBWorker(Actor):
    async def startup(self):
        self.db = Database(DATABASE['GDAX'], migrate=False)
        self.polo_db = Database(DATABASE['POLO'], migrate=False)
        self.migrate = True
        if self.migrate:
            await self.db.migrate()
            await self.polo_db.migrate()

    @concurrent
    async def insert_into_db(self, data, table_name, exchange_name=None):
        if exchange_name == "GDAX":
            await self.db.insert_into(table_name, data=data)
        if exchange_name == "POLO":
            await self.polo_db.insert_into(table_name, data=data)

    async def shutdown(self):
        pass


class BackFillTrades(Actor):
    async def startup(self):
        self.public_client = ccxt.gdax()
        self.db_worker = DBWorker()

    @concurrent
    async def backfill_trades(self, last_trade_id, current_trade_id, ccxt_product_id, product_id):
        trades = []
        missing_trade_ids = list(range(last_trade_id + 1, current_trade_id))
        number_of_request_required = math.ceil(len(missing_trade_ids) / 100)
        print("missed the following trades: " + str(missing_trade_ids))
        after_arg = max(missing_trade_ids) + 1
        recursive_count = 1
        while missing_trade_ids and recursive_count <= number_of_request_required:
            # time sleep to prevent dos on exchange
            asyncio.sleep(0.5)
            params = {}
            if after_arg:
                params = {'after': after_arg}
            product_trades = await self.public_client.fetch_trades(ccxt_product_id, params=params)
            product_trades = [product_trade['info'] for product_trade in product_trades]
            product_trades_dict = {}
            for product_trade in product_trades:
                product_trades_dict[int(product_trade['trade_id'])] = product_trade
            for missing_trade_id in missing_trade_ids:
                if missing_trade_id in product_trades_dict:
                    missing_product_trade = product_trades_dict[missing_trade_id]
                    missing_trade = {
                        "server_datetime": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%Z"),
                        "exchange_datetime": missing_product_trade['time'],
                        "sequence": "None",
                        "trade_id": missing_product_trade['trade_id'],
                        "product_id": product_id,
                        'price': missing_product_trade['price'],
                        'volume': missing_product_trade['size'],
                        'side': missing_product_trade['side'],
                        'backfilled': 'True'
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
            logging.critical("__________________________________________________________________")
            logging.critical("GDAX: Recusive Count {}".format(recursive_count))
            logging.critical("GDAX: Trades finally filled {}".format(trades_filled))
            logging.critical("GDAX: Trades finally missing {}".format(final_missing_trades))
            logging.critical("GDAX: Required Trades {}".format(missing_trade_ids))
            logging.critical("GDAX: Trades from API: {}".format(product_trades_dict.keys()))
            logging.critical("__________________________________________________________________")

            for trade in trades:
                await self.db_worker.insert_into_db(trade, "gdax_trades", exchange_name="GDAX")

    async def shutdown(self):
        pass


class Worker(BaseWorker):
    shadows = [DBWorker, BackFillTrades]
