"""
因为每天8:00 am后米筐更新前复权价, 所以此脚本在8:00 am时更新数据
"""
import pandas as pd
import rqdatac
import time
import clickhouse_driver
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
    "database.database": "vnpyzh",
    "database.host": "localhost",
    "database.port": 3306,
    "database.user": "remote",
    "database.password": "zhP@55word"
}

# 修改vt_setting.json
with open('/home/ubuntu/anaconda3/lib/python3.10/site-packages/vnpy/trader/vt_setting.json','w') as f1:
    json.dump(setting_backup,f1, indent=4, ensure_ascii=False)

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
from vnpy.trader.object import BarData


# 米筐初始化
rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )
# print(rqdatac.user.get_quota())

# 获取数据库实例
database = get_database()
conn = clickhouse_driver.connect(host='localhost', port=9000, database='common_info',user='remote',password='zhP@55word')  # 用于取trading_day数据


exchg_dict = {'SSE':Exchange.SSE, 'SZSE':Exchange.SZSE}
interval_dict = {'1m':Interval.MINUTE,
                 '1h':Interval.HOUR,
                 'd':Interval.DAILY,
                 'w':Interval.WEEKLY
                 }

fields = ['open','high','low','close','volume','total_turnover']

def get_last_trading_day(xdate):
    sql = "select * from common_info.trading_day"
    data = pd.read_sql(sql,conn)
    zdate = int(''.join(xdate.split('-')))
    last_trading_day = data[data['date'].shift(-1)==zdate]['datetime'].dt.strftime('%Y-%m-%d').values[0]
    return last_trading_day

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
        exchg = Exchange.SSE
    elif ex_rq == 'XSHG':
        exchg = Exchange.SZSE
    else:
        print('wrong stock contract: '+contract)
        exchg = ''
    return exchg

def get_excum_factor(contract, edate, edatex):
    data = rqdatac.get_ex_factor(order_book_ids=contract, start_date=edate, end_date=edatex, market='cn')
    return data

def move_df_to_mysql(imported_data:pd.DataFrame):
    bars = []
    start = None
    count = 0
    for row in imported_data.itertuples():
        bar = BarData(
              symbol=row.symbol,
              exchange=row.exchange,
              datetime=row.datetime.to_pydatetime(),
              interval=interval_dict[row.interval],
              volume=row.volume,
              open_price=row.open_price,
              high_price=row.high_price,
              low_price=row.low_price,
              close_price=row.close_price,
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


if __name__ == '__main__':
    print_date = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{print_date}: {__file__}")

    # freq = Interval.MINUTE 
    freq = '1m'
    sdate = '2021-01-01'
    edate = time.strftime("%Y-%m-%d")
    edatex = str(int(edate[:4])+1)+edate[4:]
    last_date = get_last_trading_day(edate)
    # print(last_date)
    contracts = get_contracts()
    ex_factor_data = get_excum_factor(contracts, last_date, edatex)
    # print(ex_factor_data)
    if ex_factor_data is None:
        ex_symbols = []
    else:
        ex_symbols = ex_factor_data[ex_factor_data.announcement_date==last_date]['order_book_id'].to_list()

    data_nochg = pd.DataFrame()
    for contract in contracts:  # 按一个票一个票循环，然后合并，其实也可以多个票，可以测试下怎么样速度更加快
        if contract in ex_symbols:
            print("ex contract: ", contract)
            data = get_data(contract, sdate, last_date, freq)

            # 先删除symbol的数据，再重新插入
            exchange = convert_exchange_code(contract)
            interval = Interval.MINUTE
            database.delete_bar_data(
                symbol=contract.split('.')[0],
                exchange=exchange,
                interval=interval
                )
            move_df_to_mysql(data)
        else:
            data = get_data(contract, last_date, last_date, freq)
            data_nochg = pd.concat([data_nochg, data])
        # print(data)
    if data_nochg.empty:
        pass
    else:
        move_df_to_mysql(data_nochg)
