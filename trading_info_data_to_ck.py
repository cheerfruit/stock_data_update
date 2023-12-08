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
    # max_date = pd.DataFrame()
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
    ENGINE = ReplacingMergeTree()\
    PRIMARY KEY (date)"
    client.execute(sql)
    return

def create_ex_factor_table():
    sql = "create table if not exists common_info.ex_factor ( \
    ex_date DateTime,\
    order_book_id String,\
    book_closure_date DateTime,\
    ex_cum_factor Float32,\
    ex_end_date  DateTime,\
    ex_factor Float32,\
    create_date DateTime,\
    close Float32,\
    cash Float32,\
    share Float32,\
    spread Float32,\
    remarks Float32,\
    )\
    ENGINE = ReplacingMergeTree()\
    PRIMARY KEY (book_closure_date, order_book_id)"
    client.execute(sql)
    return

def create_ex_factor_table_mysql():
    sql = "create table if not exists common_info.ex_factor ( \
    id INT AUTO_INCREMENT PRIMARY KEY,\
    ex_date date,\
    order_book_id char(20),\
    book_closure_date date,\
    ex_cum_factor float,\
    ex_end_date  date,\
    ex_factor float,\
    ex_factor_theory float,\
    create_date date,\
    cash float,\
    round_lot Int,\
    split_coefficient_from float,\
    split_coefficient_to float,\
    spread float,\
    close float,\
    remarks char(50)\
    )\
    ENGINE = InnoDB\
    "
    cursor.execute(sql)
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
    return rq_codes

def get_close_data(contract, datelist):
    close_data = pd.DataFrame()
    for date_elem in datelist:
        # print(date_elem)
        close_p = rqdatac.get_price(contract, date_elem, date_elem, frequency='1d', adjust_type='none')
        if not close_p is None:
            close_data = pd.concat([close_data, close_p])
    # print(close_data)
    return close_data

def get_last_updated_date():
    sql = "select max(ex_date) from common_info.ex_factor;"
    dt = pd.read_sql(sql, conn).values[0][0]
    return dt

def update_ex_factor_data():
    sdate = '20210101'
    edate = '20500901'
    update_sdate = get_last_updated_date()
    if update_sdate is None:
        update_sdate = '2021-01-01'
    else:
        update_sdate = update_sdate.strftime("%Y-%m-%d")
        # update_sdate = '2021-01-01'
    update_edate = time.strftime("%Y-%m-%d")
    contracts = get_contracts()
    data = pd.DataFrame()
    # print(contracts)
    for contract in contracts:
        data_tmp = rqdatac.get_ex_factor(order_book_ids=contract[0], start_date=sdate, end_date=edate,market='cn')
        if data_tmp is None:
            pass
        else:
            data_tmp = data_tmp.reset_index().set_index(['ex_date', 'order_book_id'])
            data_dividend = rqdatac.get_dividend(order_book_ids=contract[0],start_date=sdate, end_date=edate)
            data_split = rqdatac.get_split(order_book_ids=contract[0],start_date=sdate, end_date=edate)
            
            if not data_split is None:
                data_split = data_split.reset_index().set_index(['ex_dividend_date', 'order_book_id'])
                data_tmp['split_coefficient_from'] = data_split['split_coefficient_from']
                data_tmp['split_coefficient_to'] = data_split['split_coefficient_to']
                data_tmp['share'] = data_split['split_coefficient_to']/data_split['split_coefficient_from']
                data_tmp['book_closure_date'] = data_split['book_closure_date']
            else:
                data_tmp['split_coefficient_from'] = None
                data_tmp['split_coefficient_to'] = None
                data_tmp['share'] = 1
                data_tmp['book_closure_date'] = None
            if not data_dividend is None:
                data_dividend = data_dividend.reset_index().set_index(['ex_dividend_date', 'order_book_id'])
                data_tmp['cash'] = data_dividend['dividend_cash_before_tax']
                data_tmp['round_lot'] = data_dividend['round_lot']
                data_tmp['cash_pro'] = data_tmp['cash']/data_tmp['round_lot']
                data_tmp['book_closure_date'] = data_dividend['book_closure_date']
            else:
                data_tmp['cash'] = None
                data_tmp['round_lot'] = None
                data_tmp['cash_pro'] = 0
                data_tmp['book_closure_date'] = None
            # 获取收盘价
            data_tmp = data_tmp.reset_index()
            data_tmp['share'] = data_tmp['share'].fillna(1)
            close_data = get_close_data(contract[0], data_tmp['book_closure_date'].astype(str).to_list())
            data_tmp = data_tmp.set_index(['order_book_id','book_closure_date'])
            data_tmp['close'] = close_data['close']
            data_tmp['ex_factor_theory'] = data_tmp['close']/round((data_tmp['close'] - data_tmp['cash_pro'])/data_tmp['share'], 2)
            data_tmp['spread'] = data_tmp['close'] - round((data_tmp['close'] - data_tmp['cash_pro'])/data_tmp['share'], 2)
            data_tmp = data_tmp.reset_index()
            data_tmp['order_book_id'] = contract[1]
            data = pd.concat([data, data_tmp])
    # sql = 'truncate table common_info.ex_factor'
    # cursor.execute(sql)
    data['ex_date']= pd.to_datetime(data.ex_date)
    data['ex_end_date']= data['ex_end_date'].fillna(pd.to_datetime('2050-01-01', format="%Y-%m-%d"))
    data = data.replace({np.nan:None})
    data['create_date'] = pd.to_datetime(time.strftime("%Y-%m-%d"))
    data['remarks'] = None
    data_mysql = data[(data.book_closure_date>=update_sdate)&(data.book_closure_date<=update_edate)]
    if data_mysql.empty:  # 如果没有股票更新则不更新
        return
    cols_new = 'ex_date,order_book_id,book_closure_date,ex_cum_factor,ex_end_date,ex_factor,ex_factor_theory,create_date,cash,round_lot,split_coefficient_from,split_coefficient_to,spread,close,remarks'
    tablename = 'ex_factor'
    insert_into_mysql_database(data_mysql[cols_new.split(',')], tablename, cols_new)
    print("Finish inserting into mysql!")

    # 初始化表格
    sql = 'truncate table common_info.ex_factor'
    client.execute(sql)
    data['remarks'] = 0
    cols = 'ex_date,order_book_id,book_closure_date,ex_cum_factor,ex_end_date,ex_factor,create_date,remarks'
    print(data[cols.split(',')])
    insert_into_ck_database(data[cols.split(',')], tablename, cols)
    print("Finish inserting into clickhouse!")
    return


if __name__ == '__main__':
    # create_ex_factor_table()
    # create_ex_factor_table_mysql()
    print_date = time.strftime("%Y-%m-%d %H:%M:%S")
    print('#'*100)  # 这边用于data_update_error.log的记录，方便调试
    print(f"{print_date}: {__file__}")

    # get_trade_dt()
    update_trade_dt()
    update_ex_factor_data()
    print(f"{__file__}: Finished all work!")
