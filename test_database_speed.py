import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import time
import clickhouse_driver
import pymysql

# password = 'zhP@55word'

# engine1 = clickhouse_driver.connect(host='118.89.200.89', port=9000, database='vnpy_backup',user='remote',password='zhP@55word')
# engine2 = create_engine(f"clickhouse+native://remote:{urlquote(password)}@118.89.200.89:9000/vnpy_backup")

# engine3 = pymysql.connect(host='118.89.200.89', port=3306, database='vnpyzh',user='remote',password='zhP@55word')
# engine4 = create_engine(f"mysql+pymysql://remote:{urlquote(password)}@118.89.200.89:3306/vnpyzh")


engine5 = clickhouse_driver.connect(host='222.64.175.96', port=9000, database='futures',user='remote',password='zhP@55word')

t0 = time.time()
# sql = "select * from dbbardata where symbol='600073'"
# sql = "select * from dbbardata limit 50000"
# data1 = pd.read_sql(sql, engine4)
# t1 = time.time()

# sql = "select * from dbbardata where symbol='600073'"
# sql = "select * from dbbardata limit 50000"
# data2 = pd.read_sql(sql, engine2)
# t2 = time.time()

# print(data1)
# print(data2)

sql = "select * from futures.daily limit 10"
data = pd.read_sql(sql, engine5)
print(data)
# print(t1-t0)

