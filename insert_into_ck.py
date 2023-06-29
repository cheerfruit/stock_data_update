"""
此脚本用于插入无复权价格行情和复权因子, 在建库和新加股票池时运行
"""
import clickhouse_driver
import pandas as pd
import rqdatac
# from vnpy.trader.constant import Exchange, Interval

rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )
print(rqdatac.user.get_quota())

client = clickhouse_driver.Client(host='localhost', port=9000, database='stock_bar',user='remote',password='zhP@55word')

fields = ['open','high','low','close','volume','total_turnover']

def get_data(contract, sdate, edate, freq):
    data = rqdatac.get_price(order_book_ids=contract, start_date=sdate,end_date=edate,frequency=freq,fields=fields,adjust_type='pre', skip_suspended=False, market='cn')
    # print(data)
    data = data.reset_index()
    data['symbol'] = contract.split('.')[0]
    exchg = convert_exchange_code(contract)
    
    data['exchange'] = exchg
    data['interval'] = freq
    data['open_price'] = data['open']
    data['high_price'] = data['high']
    data['low_price'] = data['low']
    data['close_price'] = data['close']
    data['turnover'] = data['total_turnover']
    data['datetime'] = (pd.to_datetime(data['datetime']) - pd.Timedelta('1 minute'))#.dt.tz_localize(tz='Asia/Shanghai')
    # print(data)
    return data

def convert_exchange_code(contract):
    ex_rq = contract.split('.')[1]
    if ex_rq == 'XSHE':
        # exchg = Exchange.SSE
        exchg = 'SSE'
    elif ex_rq == 'XSHG':
        # exchg = Exchange.SZSE
        exchg = 'SZSE'
    else:
        print('wrong stock contract: '+contract)
        exchg = ''
    return exchg

def get_excum_factor(contract):
    data = rqdatac.get_ex_factor(order_book_ids=contract, start_date=sdate, end_date=edate,market='cn')
    # print(data)
    return data

def insert_into_ck_database(df):
    sql = "insert into stock_minute (datetime, date, symbol, exchange, interval, open_price, high_price,low_price,close_price,turnover,volume,ex_factor) VALUES"
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
        # print(all_tuple)
        client.execute(sql, all_tuple)
    return

def create_stock_min_table():
    sql = "create table stock_bar.stock_minute ( \
    datetime DateTime,\
    date UInt32,\
    symbol String,\
    exchange String,\
    interval String,\
    open_price Float32,\
    high_price Float32,\
    low_price Float32,\
    close_price Float32,\
    turnover UInt32,\
    volume UInt32,\
    ex_factor Float32,\
    )\
    ENGINE = MergeTree()\
    PRIMARY KEY (datetime, symbol, date)"
    client.execute(sql)
    return

def truncate_table():
    sql = 'truncate table stock_minute;'
    client.execute(sql)
    return

if __name__ == '__main__':
    # create_stock_min_table()
    # truncate_table()

    # freq = Interval.MINUTE
    freq = '1m'
    sdate = '2023-06-01'
    edate = '2023-10-20'
    contracts = ['688171.XSHG','600125.XSHG','002595.XSHE','000729.XSHE']
    ex_factor_data = get_excum_factor(contracts)
    # print(ex_factor_data)
    for contract in contracts:  # 按一个票一个票循环，其实也可以多个票，可以测试下怎么样速度更加快
        data = get_data(contract, sdate, edate, freq)
        data = data.set_index('datetime')
        if ex_factor_data is None:
            ex_data_need = pd.DataFrame()
        else:
            ex_data_need = ex_factor_data[ex_factor_data.order_book_id == contract]
        if ex_data_need.empty:
            data.loc[:,'ex_factor'] = 1
        else:
            # ex_data_need.index = pd.to_datetime(ex_data_need.index).tz_localize('Asia/Shanghai') + pd.Timedelta('09:30:00')
            ex_data_need.index = pd.to_datetime(ex_data_need.index) + pd.Timedelta('09:30:00')
            # print(ex_data_need)
            data['ex_factor'] = ex_data_need['ex_factor']
            data = data.sort_values('datetime')
            data['ex_factor'] = data['ex_factor'].fillna(1)
            # data['ex_cum_factor'] = data['ex_factor'].cumprod()
            data = data.reset_index()
            data['date'] = pd.to_datetime(data['datetime']).dt.strftime('%Y%m%d').astype(int)
            df = data[['datetime','date', 'symbol', 'exchange', 'interval','open_price', 'high_price','low_price','close_price','turnover','volume','ex_factor']]
            df['turnover'] = df['turnover'].astype(int)
            df['volume'] = df['volume'].astype(int)
            print(df)
            insert_into_ck_database(df)

