"""
此脚本用于股票前复权价每日增量插入clickhouse库, 需要每天跑
"""
import clickhouse_driver
import pandas as pd
import time
import rqdatac
import json

print('#'*100)  # 这边用于data_update_error.log的记录，方便调试
# 备用库设定
setting_backup = {
    "font.family": "微软雅黑",
    "font.size": 12,
    "log.active": True,
    "log.level": 50,
    "log.console": True,
    "log.file": True,
    "email.server": "smtp.qq.com",
    "email.port": 465,
    "email.username": "",
    "email.password": "",
    "email.sender": "",
    "email.receiver": "",
    "datafeed.name": "",
    "datafeed.username": "",
    "datafeed.password": "",
    "database.timezone": "Asia/Shanghai",
    "database.name": "mysql",
    "database.database": "vnpy_backup",
    "database.host": "localhost",
    "database.port": 3306,
    "database.user": "remote",
    "database.password": "zhP@55word"
}

# 修改vt_setting.json
with open('/home/ubuntu/.vntrader/vt_setting.json','w') as f1:
    json.dump(setting_backup,f1, indent=4, ensure_ascii=False)


from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
from vnpy.trader.object import BarData


# 米筐初始化
rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )
# print(rqdatac.user.get_quota())

# 获取数据库实例
database = get_database()

# 与clickhouse建立连接
conn = clickhouse_driver.connect(host='localhost', port=9000, database='stock_bar',user='remote',password='zhP@55word')
client = clickhouse_driver.Client(host='localhost', port=9000, database='vnpy_backup',user='remote',password='zhP@55word')

exchg_dict = {'SSE':Exchange.SSE, 'SZSE':Exchange.SZSE}
interval_dict = {'1m':Interval.MINUTE,
                 '1h':Interval.HOUR,
                 'd':Interval.DAILY,
                 'w':Interval.WEEKLY
                 }

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

def get_excum_factor(contract):
    data = rqdatac.get_ex_factor(order_book_ids=contract, start_date=edate, end_date=edatex, market='cn')
    return data

def convert_exchange_code(contract):
    ex_rq = contract.split('.')[1]
    if ex_rq == 'XSHE':
        exchg = Exchange.SZSE
    elif ex_rq == 'XSHG':
        exchg = Exchange.SSE
    else:
        print('wrong stock contract: '+contract)
        exchg = ''
    return exchg

def process_symbol_data(contract, sdate, ex_f):
    contractx = contract.split('.')[0]
    # print(contractx)
    sql = "select * from stock_minute where date>="+str(sdate)+ " and symbol='" + contractx+"';"
    data = pd.read_sql(sql,conn)
    data = data.sort_values(by='datetime')
    data['ex_cum_factor'] = data['ex_factor'].cumprod()
    data['pre_adj_factor'] = data['ex_cum_factor']/data['ex_cum_factor'].iloc[-1]/ex_f
    data['datetime'] = data['datetime'].dt.tz_localize('Asia/Shanghai')
    data['open_adj'] = data['open_price']*data['pre_adj_factor']
    data['high_adj'] = data['high_price']*data['pre_adj_factor']
    data['low_adj'] = data['low_price']*data['pre_adj_factor']
    data['close_adj'] = data['close_price']*data['pre_adj_factor']

    move_df_to_mysql(data)
    insert_df = data[['symbol','exchange','datetime','interval','volume','open_adj','high_adj','low_adj','close_adj','turnover']]
    insert_into_ck_database(insert_df)
    del data, insert_df
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
    # print(f"Insert Bar: {count} from {start} - {end}")

def create_dbbardata_table():
    sql = "create table if not exists vnpy_backup.dbbardata ( \
    symbol String,\
    exchange String,\
    datetime DateTime,\
    interval String,\
    volume Float32,\
    open_price Float32,\
    high_price Float32,\
    low_price Float32,\
    close_price Float32,\
    turnover Float32,\
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

def get_database_latest_symbols():
    sql = "select distinct(symbol),exchange from vnpy_backup.dbbardata"
    data = pd.read_sql(sql, conn)
    codes = (data['symbol']+ '.' + data['exchange']).to_list()
    rq_codes = []
    for code in codes:
        if 'SZSE' in code:
            code = code[:7]+'XSHE'
        elif 'SSE' in code:
            code = code[:7]+'XSHG'
        rq_codes.append(code)
    return rq_codes


if __name__ == '__main__':
    try:
        print_date = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{print_date}: {__file__}")

        # create_dbbardata_table()
        edate = time.strftime("%Y-%m-%d")
        if edate[4:]!='-02-29':
            edatex = str(int(edate[:4])+1)+edate[4:]
        else:
            edatex = str(int(edate[:4])+1)+'-02-28'
        # print(edatex)
        
        contracts = get_contracts()                       # 最新股票池code
        latest_symbols = get_database_latest_symbols()    # 数据库里的code
        contracts0 = list(set(latest_symbols)&(set(contracts)))  # 不发生变动的股票
        contracts1 = list(set(latest_symbols) - set(contracts))       # 剔除的股票
        contracts2 = list(set(contracts) - set(latest_symbols))       # 新增的股票

        contracts0.sort()
        contracts1.sort()
        contracts2.sort()
        print("all symbol nums: ",len(contracts0+contracts1+contracts2))
        
        ex_factor_data = get_excum_factor(contracts)
        print(ex_factor_data)
        if ex_factor_data is None:
            ex_symbols = []
        else:
            ex_factor_data['contract'] = ex_factor_data['order_book_id'].str.slice(0,6)
            ex_symbols = ex_factor_data[ex_factor_data.book_closure_date==edate]['order_book_id']
        # print(ex_symbols)
        
        for contract in (contracts0+contracts1+contracts2):
            if contract in contracts0:
                if contract in ex_symbols:
                    ex_f = ex_factor_data[ex_factor_data.order_book_id==contract]['ex_factor']
                    sdate = 20210101
                    # 删除mysql的对应symbol的数据
                    exchange = convert_exchange_code(contract)
                    interval = Interval.MINUTE
                    database.delete_bar_data(
                        symbol=contract.split('.')[0],
                        exchange=exchange,
                        interval=interval
                        )
                    # 删除ck的对应的symbol数据
                    drop_symbol(contract)
                else:
                    ex_f = 1
                    sdate = int(time.strftime("%Y%m%d"))
                # 删除后重新写入完整数据
                process_symbol_data(contract, sdate, ex_f)
            elif contract in contracts1:
                # 删除ck的对应的symbol数据
                drop_symbol(contract)
                # 删除mysql的对应symbol的数据
                exchange = convert_exchange_code(contract)
                interval = Interval.MINUTE
                database.delete_bar_data(
                    symbol=contract.split('.')[0],
                    exchange=exchange,
                    interval=interval
                    )
            else:
                print('Insert new stock: ')
                if contract in ex_symbols:
                    ex_f = ex_factor_data[ex_factor_data.order_book_id==contract]['ex_factor']
                else:
                    ex_f = 1
                sdate = 20210101
                process_symbol_data(contract, sdate, ex_f)
        print(f"{__file__}: Finished all work!")
    except:
        from send_to_wechat import WeChat
        wx = WeChat()
        wx.send_data(f"118.89.200.89:{__file__}: An error occurred! ", touser='hujinglei')
