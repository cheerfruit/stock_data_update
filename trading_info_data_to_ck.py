import rqdatac
import clickhouse_driver
import pandas as pd
import numpy as np

# 米筐初始化
rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )
print(rqdatac.user.get_quota())

client = clickhouse_driver.Client(host='localhost', port=9000, database='common_info',user='remote',password='zhP@55word')

def get_trade_dt():
    # 获取交易日信息
    start_date = '20050101'
    end_date = '20240101'
    trade_dt = rqdatac.get_trading_dates(start_date, end_date, market='cn')
    df = pd.DataFrame(trade_dt, index=np.arange(len(trade_dt)),columns=['datetime'])
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.strftime("%Y%m%d").astype(int)
    print(df)
    cols = 'date, datetime'
    tablename = 'trading_day'
    insert_into_ck_database(df[['date','datetime']], tablename, cols)
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

def create_dbbardata_table():
    sql = "create table if not exists common_info.trading_day ( \
    date UInt32,\
    datetime DateTime,\
    )\
    ENGINE = MergeTree()\
    PRIMARY KEY (date)"
    client.execute(sql)
    return

if __name__ == '__main__':
    # create_dbbardata_table()
    get_trade_dt()