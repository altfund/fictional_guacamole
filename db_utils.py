import os
import sqlite3
import aiosqlite


class Database(object):

    INSERT_SQL = "insert into {table} ({fields}) VALUES ({values});"

    def __init__(self, config, migrate=False, row_factory=None):
        self.db_name = config['DATABASE']
        self.schema_path = config['SCHEMA']

        _already_exists = os.path.exists(self.db_name)
        self.row_factory = row_factory

    async def _execute(self, sql, data, fetch=False):
        """
        Executes a sql query with a short lived db connection
        then closes the connection with it's cursor.
        """
        ret = None

        async with aiosqlite.connect(self.db_name) as conn:
            if self.row_factory:
                conn.row_factory = self.row_factory
            await conn.execute(sql, data)
            if fetch:
                ret = cur.fetchall()
            await conn.commit()
            await conn.close()
        return ret

    async def migrate(self):
        """
        Used to create database tables and schema [ miagrate it]
        using a sql script file.
        """
        with open(self.schema_path, 'r') as f:
            schema = f.read()

        async with aiosqlite.connect(self.db_name) as conn:
            await conn.executescript(schema)

        print("Migrated database with schem {}".format(self.schema_path))

    async def insert_into(self, table, data):
        """
        Insert data into table.
        param:
            - table: table name "string"
            - data: actual data to insert
        """
        sql = self.INSERT_SQL.format(
            table=table,
            fields=",".join(data.keys()),
            values=",".join([":{}".format(key) for key in data.keys()])
        )

        await self._execute(sql, data)
