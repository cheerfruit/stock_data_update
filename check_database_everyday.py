"""
此脚本用于检查数据库当天最新数据是否有问题：
1.检查股票池需要的股票是否齐全, 是否有股票缺失
2.分钟线根数检查, 理论上每天都是一样的(股票240个1min kline,期货看交易时间),不同品种做一个列表, 看是否有缺失
3.检查volume=0的数据(是否为涨跌停, 过于常见, 暂时没做)
4.检查价格异常: 价格为0, 价格特别大
5.小时线: 根数检查, 和1min合成后比较(暂时没有小时线)
6.d/w: 根数检查, 合成检查(暂时没有)
7.ohlc volume turnover oi 和其他数据源对比(mysql和ck两库对比)

tick数据检查(未完成, 后续有库了再做, 也可以放在其他脚本里跑): 
1. 每分钟理论上120个tick, 检查主力合约上tick过少的合约
2. 价格的检查
3. 检查交易日是否齐全，有没有缺天

检查步骤：
直接取出所有股票当天更新的数据进行检查！

"""
import pandas as pd
import logging
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import time

# log配置
logfile_path = '/home/ubuntu/stock_data_update/logs/check_history.log'
logger = logging.getLogger()
handler = logging.FileHandler(logfile_path, encoding='utf-8')
formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s :  %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# 和数据库建立连接
host='localhost'
user='remote'
password='zhP@55word'
conn_mysql = create_engine(f"mysql+pymysql://{user}:{urlquote(password)}@{host}:3306/vnpyzh")
conn_ck = create_engine(f"clickhouse+native://{user}:{urlquote(password)}@{host}:9000/vnpy_backup")
conn_info = create_engine(f"clickhouse+native://{user}:{urlquote(password)}@{host}:9000/common_info")

bar_num_dict = {'9':30, '10':60, '11':30, '13':60, '14':60}

def get_dabardata(starttime, endtime):
    sql = f"select * from dbbardata where datetime>='{starttime}' and datetime<='{endtime}'"
    data = pd.read_sql(sql,conn_mysql)
    return data

def get_dbbardata_backup(starttime, endtime):
    sql = f"select * from dbbardata where datetime>='{starttime}' and datetime<='{endtime}'"
    data = pd.read_sql(sql,conn_ck)
    return data

def get_trading_day():
    sql = f'select * from common_info.trading_day'
    dt_data = pd.read_sql(sql, conn_info)
    return dt_data


def check_stock(data, contracts):
    print("Start checking stock code!")
    contracts_check = set(data['symbol'].unique())
    loss = set(contracts) - contracts_check
    if len(loss)>0:
        print('Missing Stock! ', str(loss))
        logger.info(f'Missing Stock! {str(loss)}')
    print("Stock code checked!\n")
    return

def check_hourly_bar_num(data, bars_per_day):
    print('Start checking bars: ')
    # 先找出bar偏少的天数在去具体到哪一天去找具体的小时
    count = data.groupby('symbol')['id'].count()
    abnormal = count[count!=bars_per_day]
    
    # 检查具体那个时间出问题
    if not abnormal.empty:
        print(abnormal)
        logger.info(f'Wrong bar numbers!')
        logger.info(abnormal)
    print('Bars checked!\n')
    return

def check_abnormal_data(data):
    print("Start checking abnormal price")
    # 价格为0
    data = data.fillna(0)
    data['price_zero'] = (data['close_price']*data['low_price']*data['high_price']*data['open_price']==0)*1
    price_zero_count = data.groupby('date')['price_zero'].sum()
    price_zero = price_zero_count[price_zero_count!=0]

    if not price_zero.empty:
        print(f'Price equals zero!')
        print(price_zero)
        logger.info(f'Price equals zero!')
        logger.info(price_zero)
    print("Abnormal price checked!\n")
    return

def check_by_order_database(data, starttime, endtime):
    print('Start checking with other database:')
    data_backup = get_dbbardata_backup(starttime, endtime)
    data_backup = data_backup.set_index('datetime')

    # 校对价格
    price_col = ['open_price','high_price','low_price','close_price']
    price_gap = (data[price_col] - data_backup[price_col]).sum(1)
    price_gap_abnormal = price_gap[price_gap.abs()>=0.01]
    if not price_gap_abnormal.empty:
        print('Price abnormal!')
        print(price_gap_abnormal)
        logger.info('Price abnormal!')
        logger.info(price_gap_abnormal)
    print("Finished!\n")
    return

def check_by_contract(contracts):
    starttime = time.strftime("%Y-%m-%d")
    endtime = time.strftime("%Y-%m-%d")
    data = get_dabardata(starttime, endtime).set_index('datetime')
    data['date'] = data.index.strftime("%Y%m%d").astype(int)

    # 检查股票缺失情况
    check_stock(data, contracts)

    # 检查分钟线根数每天每小时60根
    check_hourly_bar_num(data, 240)

    # 检查数据异常
    check_abnormal_data(data)

    # 和其他数据源对比
    check_by_order_database(data, starttime, endtime)
    return


if __name__ == '__main__':
    print_date = time.strftime("%Y-%m-%d %H:%M:%S")
    print('#'*100)  # 这边用于data_update_error.log的记录，方便调试
    print(f"{print_date}: {__file__}")

    contracts = ['688171.XSHG','600125.XSHG','002595.XSHE','000729.XSHE']
    check_by_contract(contracts)
    