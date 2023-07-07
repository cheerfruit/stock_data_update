import rqdatac
import clickhouse_driver
import pandas as pd
import numpy as np
import pymysql
import time

# 米筐初始化
rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )
print(rqdatac.user.get_quota())

client = clickhouse_driver.Client(host='localhost', port=9000, database='common_info',user='remote',password='zhP@55word')
conn = pymysql.connect(host='localhost', port=3306, database='common_info',user='remote',password='zhP@55word')
cursor = conn.cursor()

def get_trade_dt(start_date, end_date):
    # 获取交易日信息
    trade_dt = rqdatac.get_trading_dates(start_date, end_date, market='cn')
    df = pd.DataFrame(trade_dt, index=np.arange(len(trade_dt)),columns=['datetime'])
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.strftime("%Y%m%d").astype(int)
    cols = 'date, datetime'
    tablename = 'trading_day'
    insert_into_ck_database(df[['date','datetime']], tablename, cols)
    insert_into_mysql_database(df[['date','datetime']], tablename, cols)
    return

def update_trade_dt():
    dd = int(time.strftime("%Y%m%d"))
    sql = 'select max(date) from common_info.trading_day'
    max_date = pd.read_sql(sql, conn)
    if max_date.empty:
        start_date = '20050101'
        end_date = '29240101'
        get_trade_dt(start_date, end_date)
        print("History Trading day is updated!")
        return
    elif dd>max_date.values[0]:
        start_date = dd
        end_date = '29240101'
        get_trade_dt(start_date, end_date)
        print("Trading day is updated!")
        return
    return 

def insert_into_ck_database(df, tablename, cols):
    sql = f"insert into {tablename} ("+cols+") VALUES"
    all_array = df.to_numpy()
    all_tuple = []
    count = 0
    for i in all_array:
        tmp = tuple(i)
        all_tuple.append(tmp)
        count += 1
        if count%5000 == 0:       # 每5000条存储一次
            client.execute(sql, all_tuple)
            all_tuple = []
        
    # 将没有存完的数据存储好
    if count%5000 !=0:
        client.execute(sql, all_tuple)
    return

def insert_into_mysql_database(df, tablename, cols):
    _values = ','.join(['%s']*len(cols.split(',')))
    sql = f"insert into {tablename} ("+cols+") VALUES(%s)"%_values
    all_array = df.to_numpy()
    all_tuple = []
    count = 0
    for i in all_array:
        tmp = tuple(i)
        all_tuple.append(tmp)
        count += 1
        if count%5000 == 0:       # 每5000条存储一次
            cursor.executemany(sql, all_tuple)
            conn.commit()
            all_tuple = []
        
    # 将没有存完的数据存储好
    if count%5000 !=0:
        cursor.executemany(sql, all_tuple)
        conn.commit()
    return

def create_trading_day_table():
    sql = "create table if not exists common_info.trading_day ( \
    date UInt32,\
    datetime DateTime,\
    )\
    ENGINE = MergeTree()\
    PRIMARY KEY (date)"
    client.execute(sql)
    return

def create_ex_factor_table():
    sql = "create table if not exists common_info.ex_factor ( \
    ex_date DateTime,\
    order_book_id String,\
    announcement_date DateTime,\
    ex_cum_factor Float32,\
    ex_end_date  DateTime,\
    ex_factor Float32,\
    )\
    ENGINE = MergeTree()\
    PRIMARY KEY (announcement_date, order_book_id)"
    client.execute(sql)
    return

def get_contracts():
    data = pd.read_csv('/home/ubuntu/stock_data_update/codes.csv')
    rq_codes = []
    for code in data.code.unique():
        if 'SZSE' in code:
            codex = code[:7]+'XSHE'
        elif 'SSE' in code:
            codex = code[:7]+'XSHG'
        else:
            codex = code
        rq_codes.append([codex, code])
    # print(rq_codes)
    return rq_codes

def update_ex_factor_data():
    sdate = '20210101'
    edate = '20500101'
    contracts = get_contracts()
    data = pd.DataFrame()
    # print(contracts)
    for contract in contracts:
        data_tmp = rqdatac.get_ex_factor(order_book_ids=contract[0], start_date=sdate, end_date=edate,market='cn')
        if data_tmp is None:
            pass
        else:
            data_tmp['order_book_id'] = contract[1]
            data = pd.concat([data, data_tmp])

    data.index = pd.to_datetime(data.index)
    data = data.reset_index()
    data= data.fillna(pd.to_datetime('2050-01-01', format="%Y-%m-%d"))
    data['create_date'] = pd.to_datetime(time.strftime("%Y-%m-%d"))
    data['remarks'] = None


    # 初始化表格
    sql = 'truncate table common_info.ex_factor'
    cols = 'ex_date,order_book_id,announcement_date,ex_cum_factor,ex_end_date,ex_factor,create_date,remarks'
    tablename = 'ex_factor'
    cursor.execute(sql)
    conn.commit()
    insert_into_mysql_database(data[cols.split(',')], tablename, cols)

    client.execute(sql)
    data['remarks'] = 0
    insert_into_ck_database(data[cols.split(',')], tablename, cols)

    return

if __name__ == '__main__':
    print_date = time.strftime("%Y-%m-%d %H:%M:%S")
    print('#'*100)  # 这边用于data_update_error.log的记录，方便调试
    print(f"{print_date}: {__file__}")
    # create_trading_day_table()
    # create_ex_factor_table()
    # get_trade_dt()
    update_trade_dt()
    update_ex_factor_data()
