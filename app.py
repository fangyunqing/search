import sys
import threading
import time

import pymssql
from dbutils import pooled_db

config = {
    "host": "10.103.88.88",
    "port": "1433",
    "user": "pdauser",
    "password": "pda2018@#",
    "database": "HSWarpERP"
}

b = [pooled_db.PooledDB(
    creator=pymssql,
    mincached=0,
    maxcached=6,
    maxconnections=0,
    blocking=True,
    ping=0,
    **config)]




def func(p):
    conn = p.connection()
    cur = conn.cursor()

    time.sleep(60)

    cur.execute("select 1")

    print(cur.fetchall())

    cur.close()
    conn.close()


a = threading.Thread(target=func, args=(b[0],))

a.start()

print(sys.getsizeof(b))
b.clear()
print(sys.getsizeof(b))
a.join()
