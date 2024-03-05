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
    "database.database": "vnpy_futures",
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
import traceback

try:
    # 米筐初始化
    rqdatac.init('license', 'hKzEyfcbN4O4B22wGXKfOZnOkVIyQ4fnW7VSUepZ5shkCx3Wpfkb63nMWozKudSUfMCiSx6cuWYasEyqaIVQ7a91WnYFIhxSw39GKxvHmhnlIjaSjBNncRY0Y3ZH3wWYiYbjK25Gxl9FuVkH6sA5VmnbMBmJoQHeT_seHEFYVPw=LVXNtX-oQgO2T9QDKkPx1hhlyjgrkYETwszLKzPA3ItRHWcp4crJu9dlykAOaJv4AtQuPy-THTFzBP4DfcFtIWm-W5vGQNyMMu3lD8cc1u_kxXFfihqajhijKdIi8nJvVrOexx1XVI6Vv-FdzrL0IVNY9e9GCcZ9lavQanQ4BNw=' )

    # 获取数据库实例
    database = get_database()
    conn = pymysql.connect(host='localhost', port=3306, database='common_info',user='remote',password='zhP@55word')
except Exception as e:
    print(f'{traceback.format_exc()}')
    print(e)
    from send_to_wechat import WeChat
    wx = WeChat()
    wx.send_data(f"118.89.200.89:{__file__}: An error occurred! ", touser='hujinglei')
    wx.send_data(f"118.89.200.89:{__file__}: An error occurred! ", touser='liaoyuan')

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

def get_trading_contracts_data(symbol, tradedt):
    contracts = rqdatac.futures.get_contracts(symbol, tradedt)
    # print(contracts)
    exchange = exchg_dict[symbol_cap2exchange[symbol]]
    if contracts != []:
        data = rqdatac.get_price(contracts, tradedt, tradedt, frequency='1d')
        data['exchange'] = exchange
    else:
        data = pd.DataFrame()
    return data

def get_trade_date(sdate, edate):
    sql = f"select date from common_info.trading_day where date>={sdate} and date<={edate}"
    data = pd.read_sql(sql,conn)
    trade_dts = data['date'].to_list()
    # print(data)
    return trade_dts

def get_tradedt_data(trade_dt):
    tradedt_kline = pd.DataFrame()
    # symbols = ['PF']
    for symbol in symbols:
        if symbol not in ['sctas']:
            symbol_rq = symbol.upper()
            data_tmp = get_trading_contracts_data(symbol_rq, trade_dt)
            tradedt_kline = pd.concat([tradedt_kline, data_tmp])
    return tradedt_kline

def process_data(df):
    df = df.reset_index()
    df['symbol'] = df['order_book_id']
    df['datetime'] = pd.to_datetime(df['date'])
    df['interval'] = 'd'
    df['open_price'] = df['open']
    df['high_price'] = df['high']
    df['low_price'] = df['low']
    df['close_price'] = df['close']
    df['turnover'] = df['total_turnover']
    return df

def update_singleday_data(trade_dt):
    data = get_tradedt_data(trade_dt)
    data = process_data(data)
    # print(data)
    move_df_to_mysql(data)

def update_history_daily_kline(sdate, edate):
    histtory_trade_dts = get_trade_date(sdate, edate)
    for trade_dt in histtory_trade_dts:
        update_singleday_data(str(trade_dt))

def update_daily():
    trade_dt = time.strftime("%Y%m%d")
    update_singleday_data(trade_dt)


if __name__ == '__main__':
    try:
        print_date = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{print_date}: {__file__}")

        # 更新历史数据
        # update_history_daily_kline(20240223, 20240304)  #最后的更新日期+一天

        # 每日更新
        update_daily()
        print(f"{__file__}: Finished all work!")
    except Exception as e:
        print(e)
        from send_to_wechat import WeChat
        wx = WeChat()
        wx.send_data(f"118.89.200.89:{__file__}: An error occurred! ", touser='hujinglei')
        wx.send_data(f"118.89.200.89:{__file__}: An error occurred! ", touser='liaoyuan')
