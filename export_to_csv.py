import sys
import csv
import sqlite3

from db_utils import Database
from config import DATABASE

db = Database(DATABASE['GDAX'], row_factory=sqlite3.Row)

data = db._execute("SELECT * FROM gdax_order_book", {}, fetch=True)

titles = data[0].keys()
mode = "wb" if sys.version_info < (3,) else "w"

with open('order_books.csv', mode) as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerow(titles)  # keys=title you're looking for
    writer.writerows(data)
