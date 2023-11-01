# -*- coding: utf-8 -*-
"""
因为每天8:00 am后米筐更新前复权价, 所以此脚本在8:00 am时更新数据
"""
from iFinDPy import *
import pandas as pd
import time
import json
import pymysql

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
    "database.database": "vnpy_THS",
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
pd.options.display.width = 320
pd.options.display.max_columns = None

# 获取数据库实例
database = get_database()
exchg_dict = {'SH':Exchange.SSE, 'SZ':Exchange.SZSE}

conn = pymysql.connect(host='localhost', port=3306, database='vnpy_THS',user='remote',password='zhP@55word')

# exchg_dict = {'SSE':Exchange.SSE, 'SZSE':Exchange.SZSE}

# 登录函数
def thslogindemo():
    # 输入用户的帐号和密码
    thsLogin = THS_iFinDLogin("zh35508","0f2276")
    # thsLogin = THS_iFinDLogin("zh36070","3c2759")
    # print(thsLogin)
    if thsLogin != 0:
        print('登录失败')
    else:
        print('登录成功')

def get_stock_history_1m_data(codes, startdate):
    indicators = 'open;high;low;close;volume;amount'
    HFS_parameters = 'interval:1,CPS:backword'
    enddate = time.strftime("%Y-%m-%d")
    # enddate = '2023-07-05'
    dataformat = 'format:dataframe'
    data = THS_HF(codes, indicators, HFS_parameters,startdate,enddate,dataformat)
    df = data.data
    print(data.errmsg)
    df['exchange'] = df['thscode'].str.slice(7)
    df['symbol'] = df['thscode'].str.slice(0,6)
    df['datetime'] = pd.to_datetime(df['time']).dt.tz_localize(tz='Asia/Shanghai')
    return  df

def move_df_to_mysql(imported_data:pd.DataFrame):
    bars = []
    start = None
    count = 0
    for row in imported_data.itertuples():
        bar = BarData(
              symbol=row.symbol,
              exchange=exchg_dict[row.exchange],
              datetime=row.datetime.to_pydatetime(),
              interval=Interval.MINUTE,
              volume=row.volume,
              open_price=row.open,
              high_price=row.high,
              low_price=row.low,
              close_price=row.close,
              open_interest=0,
              turnover=row.amount,
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

def get_contracts():
    data = pd.read_csv('/home/ubuntu/stock_data_update/codes.csv')
    ths_codes = []
    for code in data.code.unique():
        if 'SZSE' in code:
            code = code[:7]+'SZ'
        elif 'SSE' in code:
            code = code[:7]+'SH'
        ths_codes.append(code)
    return ths_codes

def process_data(df):
    df['pre_open'] = df['open'].shift(1)
    df['pre_volume'] = df['volume'].shift(1)
    df['pre_amount'] = df['amount'].shift(1)
    df['min'] = df['datetime'].dt.strftime('%H:%M')
    # print(df.loc[df['min'].isin(["09:31","13:01"])])
    df.loc[df['min']=="09:31",'open'] = df.loc[df['min']=="09:31",'pre_open']
    df.loc[df['min']=="09:31",'low'] = df.loc[df['min']=="09:31",['pre_open','low']].min(1)
    df.loc[df['min']=="09:31",'high'] = df.loc[df['min']=="09:31",['pre_open','high']].max(1)
    df.loc[df['min']=="09:31",'volume'] = df.loc[df['min']=="09:31",'pre_volume']+df.loc[df['min']=="09:31",'volume']
    df.loc[df['min']=="09:31",'amount'] = df.loc[df['min']=="09:31",'pre_amount']+df.loc[df['min']=="09:31",'amount']
    df.loc[df['min']=="13:01",'open'] = df.loc[df['min']=="13:01",'pre_open']
    df.loc[df['min']=="13:01",'low'] = df.loc[df['min']=="13:01",['pre_open','low']].min(1)
    df.loc[df['min']=="13:01",'high'] = df.loc[df['min']=="13:01",['pre_open','high']].max(1)
    # print(df.loc[df['min'].isin(["09:31","13:01"])])
    df = df[~df['min'].isin(["09:30","13:00"])]
    df = df.fillna(method='ffill')
    return df

def get_max_date():
    sql = "select max(datetime) from vnpy_THS.dbbardata"
    data = pd.read_sql(sql,conn)
    max_date = str(data.values[0][0]+pd.Timedelta('1d'))[:10]
    # print(max_date)
    return max_date

def get_excum_factor():
    sql = "select * from common_info.ex_factor"
    data = pd.read_sql(sql, conn)
    return data

def get_last_trading_day(xdate):
    sql = "select * from common_info.trading_day"
    data = pd.read_sql(sql,conn)
    zdate = int(''.join(xdate.split('-')))
    data['datetime'] = pd.to_datetime(data['datetime'])
    last_trading_day = data[data['date'].shift(-1)==zdate]['datetime'].dt.strftime('%Y-%m-%d').values[0]
    return last_trading_day

def get_ex_symbols(last_date):
    # 检查是否有除权除息
    ex_factor_data = get_excum_factor()
    # print(ex_factor_data)
    if ex_factor_data is None:
        ex_symbols = []
    else:
        ex_symbols = ex_factor_data[ex_factor_data.book_closure_date==last_date]['order_book_id'].to_list()
    ex_codes = []
    for code in ex_symbols:
        if 'SZSE' in code:
            code = code[:7]+'SZ'
        elif 'SSE' in code:
            code = code[:7]+'SH'
        ex_codes.append(code)
    return ex_codes

def get_database_latest_symbols():
    sql = "select distinct(symbol),exchange from dbbardata"
    data = pd.read_sql(sql, conn)
    codes = (data['symbol']+ '.' + data['exchange']).to_list()
    ths_codes = []
    for code in codes:
        if 'SZSE' in code:
            code = code[:7]+'SZ'
        elif 'SSE' in code:
            code = code[:7]+'SH'
        ths_codes.append(code)
    return ths_codes

def main():
    # 登录函数
    thslogindemo()
    # ths_codes = get_contracts()
    contracts = get_contracts()
    latest_symbols = get_database_latest_symbols()                # 数据库里的code
    contracts0 = list(set(latest_symbols)&(set(contracts)))       # 不发生变动的股票
    contracts1 = list(set(latest_symbols) - set(contracts))       # 剔除的股票
    contracts2 = list(set(contracts) - set(latest_symbols))       # 新增的股票
    startdate = get_max_date()
    edate = time.strftime("%Y-%m-%d")
    last_date = get_last_trading_day(edate)
    ex_codes = get_ex_symbols(last_date)
    print(last_date)
    print("Ex_codes: ", ex_codes)
    
    data_all = pd.DataFrame()
    for ths_code in (contracts0+contracts1+contracts2):
        print(ths_code)
        if ths_code in contracts0:
            if ths_code not in ex_codes:
                data = get_stock_history_1m_data(ths_code,startdate)
                data = process_data(data)
                data['datetime'] = data['datetime'] - pd.Timedelta('1min')
                data_all = pd.concat([data_all, data])
                if data_all.shape[0]>10000:
                    move_df_to_mysql(data_all)
                    del data_all
                    data_all = pd.DataFrame()
            else:
                data = get_stock_history_1m_data(ths_code, '2021-01-01')
                data = process_data(data)
                data['datetime'] = data['datetime'] - pd.Timedelta('1min')

                # 先删除对应symbol的数据
                exchange = exchg_dict[ths_code.split('.')[1]]
                interval = Interval.MINUTE
                database.delete_bar_data(
                    symbol=ths_code.split('.')[0],
                    exchange=exchange,
                    interval=interval
                    )
                # 然后插入数据
                move_df_to_mysql(data)
                del data
        elif ths_code in contracts1:
            exchange = exchg_dict[ths_code.split('.')[1]]
            interval = Interval.MINUTE
            database.delete_bar_data(
                symbol=ths_code.split('.')[0],
                exchange=exchange,
                interval=interval
                )
        else:
            data = get_stock_history_1m_data(ths_code, '2021-01-01')
            data = process_data(data)
            data['datetime'] = data['datetime'] - pd.Timedelta('1min')
            move_df_to_mysql(data)
            del data

    if not data_all.empty:
        move_df_to_mysql(data_all)


if __name__ == '__main__':
    print_date = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{print_date}: {__file__}")
    main()
    print(f"{__file__}: Finished all work!")

