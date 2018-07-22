from db_utils import Database
from config import DATABASE

from arq import Actor, BaseWorker, concurrent


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
        # print(exchange_name)
        if exchange_name == "GDAX":
            await self.db.insert_into(table_name, data=data)
        if exchange_name == "POLO":
            await self.polo_db.insert_into(table_name, data=data)

    async def shutdown(self):
        pass

class Worker(BaseWorker):
    shadows = [DBWorker]
