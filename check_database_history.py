"""
此脚本用于检查数据库历史数据是否有问题：
1.检查交易日是否齐全，有没有缺天
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
历史数据较多, 所以是单个票逐一检查, 然后把发现的错误写在log里
"""
import pandas as pd
# import clickhouse_driver
# import pymysql
import logging
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote

logfile_path = '/home/ubuntu/stock_data_update/logs/check_history.log'
# FORMAT = '%(asctime)s - %(message)s'
# logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
handler = logging.FileHandler(logfile_path, encoding='utf-8')
formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s :  %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
# print(logfile_path)

host='localhost'
port=3306
user='remote'
password='zhP@55word'
database='vnpyzh'

conn_mysql = create_engine(f"mysql+pymysql://{user}:{urlquote(password)}@{host}:{port}/{database}")
conn_ck = create_engine(f"clickhouse+native://{user}:{urlquote(password)}@{host}:9000/vnpy_backup")
conn_info = create_engine(f"clickhouse+native://{user}:{urlquote(password)}@{host}:9000/common_info")

bar_num_dict = {'9':30, '10':60, '11':30, '13':60, '14':60}

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

def get_dabardata(contract):
    sql = f"select * from dbbardata where symbol='{contract[:6]}'"
    data = pd.read_sql(sql,conn_mysql)
    return data

def get_dbbardata_backup(contract):
    sql = f"select * from dbbardata where symbol='{contract[:6]}'"
    data = pd.read_sql(sql,conn_ck)
    return data

def get_trading_day():
    sql = f'select * from common_info.trading_day'
    dt_data = pd.read_sql(sql, conn_info)
    return dt_data

def check_trading_day(data, trade_dt, sdate, edate):
    print('\nStart checking trading day:')
    check_set = set(data['date'].unique())
    tradedt_set = set(trade_dt[(trade_dt['date']>=sdate)&(trade_dt['date']<=edate)]['date'].to_list())

    a = check_set - tradedt_set
    b = tradedt_set - check_set
    if len(a)>0:
        print(f"Wrong Date: {str(a)}")
        logger.info(f"Wrong Date: {data.symbol.iloc[0]} {str(a)}")
    if len(b)>0:
        print(f"Missing Trading Date: {str(b)}")
        logger.info(f"Missing Trading Date: {data.symbol.iloc[0]} {str(b)}")
    else:
        print('Trading day checked!\n')

def check_hourly_bar_num(data, bars_per_day):
    print('Start check bars: ')
    # 先找出bar偏少的天数在去具体到哪一天去找具体的小时
    count = data.groupby('date')['symbol'].count()
    abnormal = count[count!=bars_per_day]
    
    # 检查具体那个时间出问题
    if not abnormal.empty:
        print(abnormal)
        logger.info(f'Wrong bar numbers! Code: {data.symbol.iloc[0]}')
        logger.info(abnormal)
    print('Bars checked!\n')
    return

def check_abnormal_data(data):
    print("Start check abnormal price")
    # 价格为0
    data = data.fillna(0)
    data['price_zero'] = (data['close_price']*data['low_price']*data['high_price']*data['open_price']==0)*1
    price_zero_count = data.groupby('date')['price_zero'].sum()
    price_zero = price_zero_count[price_zero_count!=0]

    if not price_zero.empty:
        print(f'Price equals zero! Code: {data.symbol.iloc[0]}')
        print(price_zero)
        logger.info(f'Price equals zero! Code: {data.symbol.iloc[0]}')
        logger.info(price_zero)
    print("Abnormal price checked!\n")
    return

def check_by_order_database(data, contract):
    print('Start check with other database:')
    data_backup = get_dbbardata_backup(contract)
    data_backup = data_backup.set_index('datetime')

    # 校对价格
    price_col = ['open_price','high_price','low_price','close_price']
    price_gap = (data[price_col] - data_backup[price_col]).sum(1)
    price_gap_abnormal = price_gap[price_gap.abs()>=0.01]
    if not price_gap_abnormal.empty:
        print(f'Price abnormal! Code: {data.symbol.iloc[0]}')
        print(price_gap_abnormal)
        logger.info(f'Price abnormal! Code: {data.symbol.iloc[0]}')
        logger.info(price_gap_abnormal)
    print("Finished!\n")
    return

def check_by_contract(contract,trade_dt):
    data = get_dabardata(contract).set_index('datetime')
    data['date'] = data.index.strftime("%Y%m%d").astype(int)
    sdate = int(data.index.min().strftime("%Y%m%d"))
    edate = int(data.index.max().strftime("%Y%m%d"))

    # 检查交易日缺失情况
    check_trading_day(data, trade_dt, sdate, edate)

    # 检查分钟线根数每天每小时60根
    check_hourly_bar_num(data, 240)

    # 检查数据异常
    check_abnormal_data(data)

    # 和其他数据源对比
    check_by_order_database(data, contract)
    return


if __name__ == '__main__':
    # contracts = ['688171.XSHG','600125.XSHG','002595.XSHE','000729.XSHE']
    contracts = get_contracts()
    trade_dt = get_trading_day()

    for contract in contracts:
        print('#'*150)
        print(f'symbol: {contract}')
        check_by_contract(contract,trade_dt)
        
