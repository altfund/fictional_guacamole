import sys
import sqlite3
import csv

conn=sqlite3.connect("websocket_data.db")
c=conn.cursor()
conn.row_factory=sqlite3.Row
crsr=conn.execute("SELECT * From gdax_order_book")
row=crsr.fetchone()
titles=row.keys()

data = c.execute("SELECT * FROM gdax_order_book")
if sys.version_info < (3,):
    f = open('order_books.csv', 'wb')
else:
    f = open('order_books.csv', 'w', newline="")

writer = csv.writer(f,delimiter=',')
writer.writerow(titles)  # keys=title you're looking for
# write the rest
writer.writerows(data)
f.close()
