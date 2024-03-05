"""
此脚本用于插入无复权价格行情和复权因子, 在建库和新加股票池时运行
"""
import clickhouse_driver
import pandas as pd
import rqdatac
import time
print_date = time.strftime("%Y-%m-%d %H:%M:%S")
print('#'*100)  # 这边用于data_update_error.log的记录，方便调试
print(f"{print_date}: {__file__}")

rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )
print(rqdatac.user.get_quota())

client = clickhouse_driver.Client(host='localhost', port=9000, database='stock_bar',user='remote',password='zhP@55word')
conn = clickhouse_driver.connect(host='localhost', port=9000, database='stock_bar',user='remote',password='zhP@55word') 

fields = ['open','high','low','close','volume','total_turnover']

def get_contracts():
    data = pd.read_csv('/home/ubuntu/stock_data_update/codes.csv')
    rq_codes = []
    for code in data.code.unique():
        if 'SZSE' in code:
            code = code[:7]+'XSHE'
        elif 'SSE' in code:
            code = code[:7]+'XSHG'
        rq_codes.append(code)
    # print(rq_codes)
    return rq_codes

def get_data(contract, sdate, edate, freq):
    data = rqdatac.get_price(order_book_ids=contract, start_date=sdate,end_date=edate,frequency=freq,fields=fields,adjust_type='none', skip_suspended=False, market='cn')
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
    return data

def convert_exchange_code(contract):
    ex_rq = contract.split('.')[1]
    if ex_rq == 'XSHE':
        exchg = 'SZSE'
    elif ex_rq == 'XSHG':
        exchg = 'SSE'
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
    turnover Float32,\
    volume Float32,\
    ex_factor Float32,\
    )\
    ENGINE = ReplacingMergeTree()\
    PRIMARY KEY (datetime, symbol, date)"
    client.execute(sql)
    return

def truncate_table():
    sql = 'truncate table stock_minute;'
    client.execute(sql)
    return

def get_database_last_date():
    sql = "select max(datetime) from stock_bar.stock_minute"
    data = pd.read_sql(sql, conn)
    max_date = str(data.values[0][0])[:10]
    return max_date

def get_database_latest_symbols():
    sql = "select distinct(symbol),exchange from stock_bar.stock_minute"
    data = pd.read_sql(sql, conn)
    codes = (data['symbol']+ '.' + data['exchange']).to_list()
    rq_codes = []
    for code in codes:
        if 'SZSE' in code:
            code = code[:7]+'XSHE'
        elif 'SSE' in code:
            code = code[:7]+'XSHG'
        rq_codes.append(code)
    # print(rq_codes)
    # print(len(rq_codes))
    return rq_codes

def drop_data_by_symbol(symbol):
    sql = "delete from stock_bar.stock_minute where symbol='{symbol}'"
    client.execute(sql)
    return

if __name__ == '__main__':
    try:
        # create_stock_min_table()
        freq = '1m'
        sdate = str(pd.to_datetime(get_database_last_date()) +pd.Timedelta('1d'))[:10]
        print(sdate)
        edate = time.strftime("%Y-%m-%d")

        contracts = get_contracts()                       # 最新股票池code
        latest_symbols = get_database_latest_symbols()    # 数据库里的code
        contracts0 = list(set(latest_symbols)&(set(contracts)))  # 不发生变动的股票
        contracts1 = list(set(latest_symbols) - set(contracts))       # 剔除的股票
        contracts2 = list(set(contracts) - set(latest_symbols))       # 新增的股票
        contracts0.sort()
        contracts1.sort()
        contracts2.sort()

        ex_factor_data = get_excum_factor(contracts)
        for contract in (contracts0+contracts1+contracts2):  # 按一个票一个票循环，其实也可以多个票，可以测试下怎么样速度更加快
            # print(contract)
            if contract in contracts0:
                data = get_data(contract, sdate, edate, freq)
            elif contract in contracts2:
                data = get_data(contract, '2005-01-01', edate, freq)
            else:
                # 删除对应symbol的数据
                drop_data_by_symbol(contract)
                continue

            data = data.set_index('datetime')
            # print(data)
            if ex_factor_data is None:
                ex_data_need = pd.DataFrame()
            else:
                ex_data_need = ex_factor_data[ex_factor_data.order_book_id == contract]
            
            if ex_data_need.empty:
                data.loc[:,'ex_factor'] = 1
            else:
                ex_data_need.index = pd.to_datetime(ex_data_need.index) + pd.Timedelta('09:30:00')
                data['ex_factor'] = ex_data_need['ex_factor']
                data = data.sort_values('datetime')
                data['ex_factor'] = data['ex_factor'].fillna(1)

            data = data.reset_index()
            data['date'] = pd.to_datetime(data['datetime']).dt.strftime('%Y%m%d').astype(int)
            df = data[['datetime','date', 'symbol', 'exchange', 'interval','open_price', 'high_price','low_price','close_price','turnover','volume','ex_factor']]
            insert_into_ck_database(df)
            del df,data    # 必须删除, 否则容易因为内存溢出被系统killed
        
        print(f"{__file__}: Finished all work!")
    except:
        from send_to_wechat import WeChat
        wx = WeChat()
        wx.send_data(f"118.89.200.89:{__file__}: An error occurred! ", touser='hujinglei')
