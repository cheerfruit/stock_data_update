# -*- coding: utf-8 -*-
from iFinDPy import *
from datetime import datetime
import pandas as pd
import time
import json
from threading import Thread,Lock,Semaphore
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
from vnpy.trader.object import BarData

pd.options.display.width = 320
pd.options.display.max_columns = None

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

# 修改vt_setting.json并保存原有信息
f = open('/home/ubuntu/anaconda3/lib/python3.10/site-packages/vnpy/trader/vt_setting.json')
setting_now = json.load(f)
with open('/home/ubuntu/anaconda3/lib/python3.10/site-packages/vnpy/trader/vt_setting_backup.json','w') as f0:
    json.dump(setting_now, f0, indent=4, ensure_ascii=False)

with open('/home/ubuntu/anaconda3/lib/python3.10/site-packages/vnpy/trader/vt_setting.json','w') as f1:
    json.dump(setting_backup,f1, indent=4, ensure_ascii=False)

# import sys
# sys.exit()

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
    startdate = '2023-07-01'
    enddate = time.strftime("%Y-%m-%d")
    dataformat = 'format:dataframe'
    data = THS_HF(codes, indicators, HFS_parameters,startdate,enddate,dataformat)
    df = data.data
    # print(df)
    df['exchange'] = df['thscode'].str.slice(7)
    df['symbol'] = df['thscode'].str.slice(0,6)
    df['datetime'] = pd.to_datetime(df['time']).dt.tz_localize(tz='Asia/Shanghai')
    # print(df)
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
    return ','.join(ths_codes[:2])

def main():
    # 登录函数
    thslogindemo()
    ths_codes = get_contracts()
    print(ths_codes)
    # 经测试，所有股票一起取会取到None, 取股票只数有上限，现在按10个取
    data = get_stock_history_1m_data(ths_codes)   
    # data = get_stock_history_1m_data(ths_codes)
    print(data)
    print(data[data.volume.isna()])
    # move_df_to_mysql(data)


if __name__ == '__main__':
    # try:
    main()
    # except Exception as e:
    #     print(e)

    # 恢复vt_setting到原来的信息
    with open('/home/ubuntu/anaconda3/lib/python3.10/site-packages/vnpy/trader/vt_setting.json','w') as f2:
        json.dump(setting_now,f2, indent=4, ensure_ascii=False)

