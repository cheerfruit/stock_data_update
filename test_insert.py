import rqdatac
import pandas as pd
import pymysql
import json
import time
from configs import *

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
    "database.database": "vnpy_crypto",
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


# 获取数据库实例
database = get_database()

def get_data(filepath):
    data = pd.read_csv(filepath)
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
              open_interest=row.open_interest,
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

def run(filename):
    data = get_data(filename)
    data['interval'] = '1m'
    data['exchange'] = Exchange.LOCAL
    data['datetime'] = pd.to_datetime(data['datetime'])
    # print(data)
    move_df_to_mysql(data)

if __name__ == '__main__':
    # data = get_data('~/BTCBUSD.csv')
    filename = '~/ETHBUSD.csv'
    run(filename)
    
