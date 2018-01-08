import os
import sqlite3


class Database(object):
    
    INSERT_SQL = "insert into {table} ({fields}) VALUES ({values});"

    def __init__(self, config, migrate=False, row_factory=None):
        self.db_name = config['DATABASE']
        self.schema_path = config['SCHEMA']
        
        _already_exists = os.path.exists(self.db_name)
        self.row_factory = row_factory
        
        if migrate:
            self.migrate()

    def _execute(self, sql, data, fetch=False):
        """
        Executes a sql query with a short lived db connection
        then closes the connection with it's cursor.
        """
        ret = None

        with sqlite3.connect(self.db_name) as conn:
            if self.row_factory:
                conn.row_factory = self.row_factory
            cur = conn.cursor()
            cur.execute(sql, data)
            if fetch:
                ret = cur.fetchall()
            conn.commit()
            cur.close()
        
        return ret

    def migrate(self):
        """
        Used to create database tables and schema [ miagrate it]
        using a sql script file.
        """
        with open(self.schema_path, 'r') as f:
            schema = f.read()

        with sqlite3.connect(self.db_name) as conn:
            conn.executescript(schema)

        print("Migrated database with schem {}".format(self.schema_path))

    def insert_into(self, table, data):
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

        self._execute(sql, data)
