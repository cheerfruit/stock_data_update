"""
此脚本用于插入所有股票的前复权数据, 建库时或者增加票池时跑
"""
import clickhouse_driver
import pandas as pd
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
from vnpy.trader.object import BarData
import time
import rqdatac

# 米筐初始化
rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )
print(rqdatac.user.get_quota())

# 获取数据库实例
database = get_database()
# database = get_database_backup('mysql')

# 与clickhouse建立连接
conn = clickhouse_driver.connect(host='localhost', port=9000, database='stock_bar',user='remote',password='zhP@55word')
client = clickhouse_driver.Client(host='localhost', port=9000, database='vnpy_backup',user='remote',password='zhP@55word')

exchg_dict = {'SSE':Exchange.SSE, 'SZSE':Exchange.SZSE}
interval_dict = {'1m':Interval.MINUTE,
                 '1h':Interval.HOUR,
                 'd':Interval.DAILY,
                 'w':Interval.WEEKLY
                 }


def get_excum_factor(contract):
    data = rqdatac.get_ex_factor(order_book_ids=contract, start_date=edate, end_date=edatex, market='cn')
    return data

def process_symbol_data(contract, ex_f):
    contractx = contract.split('.')[0]
    print(contractx)
    sql = "select * from stock_minute where date>"+str(sdate)+ " and symbol='" + contractx+"';"
    data = pd.read_sql(sql,conn)
    print(data)
    data['ex_cum_factor'] = data['ex_factor'].cumprod()
    data['pre_adj_factor'] = data['ex_cum_factor']/data['ex_cum_factor'].iloc[-1]/ex_f
    data['datetime'] = data['datetime'].dt.tz_localize('Asia/Shanghai')
    # print(data)
    data['open_adj'] = data['open_price']*data['pre_adj_factor']
    data['high_adj'] = data['high_price']*data['pre_adj_factor']
    data['low_adj'] = data['low_price']*data['pre_adj_factor']
    data['close_adj'] = data['close_price']*data['pre_adj_factor']
    print(data)
    move_df_to_mysql(data)
    insert_df = data[['symbol','exchange','datetime','interval','volume','open_adj','high_adj','low_adj','close_adj','turnover']]
    insert_into_ck_database(insert_df)
    return

def insert_into_ck_database(df):
    sql = "insert into dbbardata (symbol,exchange,datetime,interval,volume,open_price,high_price,low_price,close_price,turnover) VALUES"
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

def move_df_to_mysql(imported_data:pd.DataFrame):
    bars = []
    start = None
    count = 0
    for row in imported_data.itertuples():
        bar = BarData(
              symbol=row.symbol,
              exchange=exchg_dict[row.exchange],
              datetime=row.datetime.to_pydatetime(),
              interval=interval_dict[row.interval],
              volume=row.volume,
              open_price=row.open_adj,
              high_price=row.high_adj,
              low_price=row.low_adj,
              close_price=row.close_adj,
              open_interest=0,
              turnover=row.turnover,
              gateway_name="DB",
        )
        bars.append(bar)

        # do some statistics
        count += 1
        if not start:
            start = bar.datetime
    end = bar.datetime

    # insert into database
    database.save_bar_data(bars)
    print(f"Insert Bar: {count} from {start} - {end}")

def create_dbbardata_table():
    sql = "create table if not exists vnpy_backup.dbbardata ( \
    symbol String,\
    exchange String,\
    datetime DateTime,\
    interval String,\
    volume UInt32,\
    open_price Float32,\
    high_price Float32,\
    low_price Float32,\
    close_price Float32,\
    turnover UInt32,\
    )\
    ENGINE = MergeTree()\
    PRIMARY KEY (symbol, exchange, datetime, interval)"
    client.execute(sql)
    return

def truncate_table():
    sql = 'truncate table dbbardata;'
    client.execute(sql)
    return

def drop_symbol(contract):
    sql = "delete from dbbardata where symbol='"+contract+"'"
    client.execute(sql)
    return


if __name__ == '__main__':
    # create_dbbardata_table()
    # truncate_table()

    sdate = 20210101
    edate = time.strftime("%Y-%m-%d")
    edatex = str(int(edate[:4])+1)+edate[4:]
    contracts = ['688171.XSHG','600125.XSHG','002595.XSHE','000729.XSHE']

    ex_factor_data = get_excum_factor(contracts)
    ex_factor_data['contract'] = ex_factor_data['order_book_id'].str.slice(0,6)
    # print(ex_factor_data)
    if ex_factor_data is None:
        ex_symbols = []
    else:
        ex_symbols = ex_factor_data[ex_factor_data.announcement_date==edate]['order_book_id']
    # print(ex_symbols)
    for contract in contracts:
        if contract in ex_symbols:
            ex_f = ex_factor_data[ex_factor_data.order_book_id==contract]['ex_factor']
        else:
            ex_f = 1
        process_symbol_data(contract, ex_f)
