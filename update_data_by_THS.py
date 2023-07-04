# -*- coding: utf-8 -*-
from iFinDPy import *
from datetime import datetime
import pandas as pd
import time
from threading import Thread,Lock,Semaphore
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
from vnpy.trader.object import BarData

pd.options.display.width = 320
pd.options.display.max_columns = None

# sem = Semaphore(5)  # 此变量用于控制最大并发数
# dllock = Lock()     # 此变量用来控制实时行情推送中落数据到本地的锁

# 获取数据库实例
database = get_database()
exchg_dict = {'SH':Exchange.SSE, 'SZ':Exchange.SZSE}


# 登录函数
def thslogindemo():
    # 输入用户的帐号和密码
    thsLogin = THS_iFinDLogin("zh35508","0f2276")
    print(thsLogin)
    if thsLogin != 0:
        print('登录失败')
    else:
        print('登录成功')

def get_stock_history_1m_data(codes):
    # indicators = 'open;high;low;close;volume;amount;ths_af_stock'
    indicators = 'open;high;low;close;volume;amount'
    HFS_parameters = 'interval:1,CPS:backword'
    startdate = '2023-06-25'
    enddate = time.strftime("%Y-%m-%d")
    dataformat = 'format:dataframe'
    data = THS_HF(codes, indicators, HFS_parameters,startdate,enddate,dataformat)
    df = data.data
    print(df)
    # df = pd.read_csv('~/test_thsdata', sep='\t')
    df['exchange'] = df['thscode'].str.slice(7)
    df['symbol'] = df['thscode'].str.slice(0,6)
    df['datetime'] = pd.to_datetime(df['time']).dt.tz_localize(tz='Asia/Shanghai')
    print(df)
    return 

def move_df_to_mysql(imported_data:pd.DataFrame):
    bars = []
    start = None
    count = 0
    for row in imported_data.itertuples():
        bar = BarData(
              symbol=row.symbol,
              exchange=exchg_dict(row.exchange),
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


def main():
    # 登录函数
    thslogindemo()
    data = get_stock_history_1m_data('600004.SH')
    # move_df_to_mysql(data)


if __name__ == '__main__':
    main()
    # data = pd.read_csv('~/test_thsdata', sep='\t')
    # data['close_pre_adj'] = data['close']*data['ths_af_stock']/data['ths_af_stock'].iloc[-1]
    # data = data.set_index('time')
    # # print(data)
    # data.index = pd.to_datetime(data.index)
    # print(data.loc['2020-08-03':])
    # data1 = pd.read_csv('~/test_thsdata_adj', sep='\t',index_col='time')
    # print(data1.loc['2020-08-03':])


