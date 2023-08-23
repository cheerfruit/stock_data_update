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
import clickhouse_driver
import pymysql

# receiver = 'hujinglei'

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
conn_mysql = pymysql.connect(host='localhost', port=3306, database='vnpyzh',user='remote',password='zhP@55word')
conn_ck = clickhouse_driver.connect(host='localhost', user='remote', password='zhP@55word', port=9000, database='vnpy_backup')
conn_info = clickhouse_driver.connect(host='localhost', user='remote', password='zhP@55word', port=9000, database='common_info')

bar_num_dict = {'9':30, '10':60, '11':30, '13':60, '14':60}

def get_last_trading_day(xdate):
    sql = "select * from common_info.trading_day"
    data = pd.read_sql(sql,conn_info)
    zdate = int(''.join(xdate.split('-')))
    last_trading_day = data[data['date'].shift(-1)==zdate]['datetime'].dt.strftime('%Y-%m-%d').values[0]
    return last_trading_day

def get_contracts():
    data = pd.read_csv('/home/ubuntu/stock_data_update/codes.csv')
    rq_codes = []
    for code in data.code.unique():
        codex = code[:6]
        rq_codes.append(codex)
    # print(rq_codes)
    return rq_codes

def get_dabardata(starttime, endtime):
    sql = f"select * from dbbardata where datetime>='{starttime} 09:00:00' and datetime<='{endtime} 23:59:00'"
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
    # print(contracts_check)
    loss = set(contracts) - contracts_check
    if len(loss)>0:
        print('Missing Stock! ', str(loss))
        logger.info(f'Missing Stock! {str(loss)}')
        # wechat.send_data(f'Missing Stock! {str(loss)}', touser=receiver)
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
        # wechat.send_data(f'Wrong bar numbers!', touser=receiver)
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
        # wechat.send_data('Price equals zero!', touser=receiver)
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
        # wechat.send_data('Price abnormal!', touser=receiver)
    print("Finished!\n")
    return

def check_by_contract(contracts):
    last_date = get_last_trading_day(time.strftime("%Y-%m-%d"))
    starttime = last_date
    endtime = last_date
    data = get_dabardata(starttime, endtime).set_index('datetime')
    # print(data)
    if data.empty:
        print('Data is not updated! date: ', last_date)
    data['date'] = data.index.strftime("%Y%m%d").astype(int)

    # 检查股票缺失情况
    check_stock(data, contracts)

    # 检查分钟线根数每天每小时60根
    check_hourly_bar_num(data, 240)

    # 检查数据异常
    check_abnormal_data(data)

    # 和其他数据源对比
    check_by_order_database(data, starttime, endtime)
    # wechat.send_data('Database Checked!', touser=receiver)
    return

if __name__ == '__main__':
    print_date = time.strftime("%Y-%m-%d %H:%M:%S")
    print('#'*100)  # 这边用于data_update_error.log的记录，方便调试
    print(f"{print_date}: {__file__}")
    logger.info('#'*100)
    logger.info(f"{print_date}: {__file__}")

    contracts = get_contracts()
    check_by_contract(contracts)
    print(f"{__file__}: Finished all work!")
    logger.info(f"{__file__}: Finished all work!")

