import os
import sqlite3


class Database(object):

    def __init__(self, config):
        self.db_name = config['DATABASE']
        self.schema_path = config['SCHEMA']
        
        _already_exists = os.path.exists(self.db_name)

        self.migrate() # should check before migrating 

    def _execute(self, sql, data, fetch=False):
        """
        Executes a sql query with a short lived db connection
        then closes the connection with it's cursor.
        """
        ret = None

        with sqlite3.connect(self.db_name) as conn:
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

    def insert(self, table, data):
        """
        Insert data into table.
        param:
            - table: table name "string"
            - data: actual data to insert 
        """
        pass

    def get(self, query):
        """
        Retrive data from table.
        """
        pass 