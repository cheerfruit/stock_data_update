import pymysql
import pandas as pd
import time
from configs import *

print('#'*100)  # 这边用于data_update_error.log的记录，方便调试

conn = pymysql.connect(host='43.159.138.138', port=3306, database='firstrate',user='zh',password='zhP@55word')
cursor = conn.cursor()


def get_daily_data(today):
    sql = f"select * from vnpy_futures.dbbardata where datetime='{today}' and `interval`='d'"
    data = pd.read_sql(sql, conn)
    return data

def get_last_ex_date(symbol, datex):
    # 换月后20个自然日内不换月（换月标记）
    date = str(datex)[:4]+'-'+str(datex)[4:6]+'-'+str(datex)[6:]
    sql = f"select book_closure_date, remarks from common_info.ex_factor_futures where order_book_id='{symbol}' and ex_date<='{date}'"
    last_ex_date = pd.read_sql(sql, conn)
    if last_ex_date.empty:    # 表明合约在以往没有历史数据，是新品种
        return True, None
    else:
        lastdate = max(last_ex_date['book_closure_date'].to_list())
        if pd.to_datetime(date) - pd.to_datetime(lastdate) <=pd.Timedelta('20days'):
            return False, None
        else:
            contfut = last_ex_date[last_ex_date['book_closure_date']==lastdate]['remarks'].to_list()[-1].split(' ')[-1]
            # print(contfut)
            return True, contfut

def select_contfut(contracts):
    min_date = 10000
    contfut = None
    for contract in contracts:
        dead_date = int(contract[-4:])
        if dead_date<min_date:
            min_date = dead_date
            contfut = contract
    return contfut

def get_next_trading_day(date):
    # sql = f"select date from common_info.trading_day where date>{date} and date<={date+100}"
    # data = pd.read_sql(sql,conn)
    # next_day = data['date'].min()
    # print(date, next_day)
    next_day =  date
    return next_day

def check_contfut(data, symbol, today, vt_symbol):
    """The contfut changes when another contract's volume and open interest is large than half of the contfut's"""
    status, contfut = get_last_ex_date(vt_symbol, today)
    if not status:    # 合约刚换月，不检查是否换月
        return []
    if contfut is None:
        contfut_new = data[data['volume'] == data['volume'].max()]['symbol'].to_list()[0]
        remarks = f"{today} switch None to {contfut_new}"
        next_td_day = pd.to_datetime(str(get_next_trading_day(today)), format="%Y%m%d")
        today_dt = pd.to_datetime(today, format="%Y%m%d")
        symbol_ex_info = [next_td_day, vt_symbol, today_dt, None, None, None, today_dt, None, None, None, None, None, None, remarks]
        return symbol_ex_info
    contfut_data = data[data.symbol==contfut]
    if contfut_data.empty:
        contfut_new = data[data['volume'] == data['volume'].max()]['symbol'].to_list()[0]
        remarks = f"{today} switch {contfut} to {contfut_new}"
        next_td_day = pd.to_datetime(str(get_next_trading_day(today)), format="%Y%m%d")
        today_dt = pd.to_datetime(today, format="%Y%m%d")
        symbol_ex_info = [next_td_day, vt_symbol, today_dt, None, None, None, today_dt, None, None, None, None, None, None, remarks]
    else:
        multi = 1
        contfut_close = contfut_data['close_price'].to_list()[0]   # 用于计算价差
        volume_thresh = multi*contfut_data['volume'].to_list()[0]
        oi_thresh = multi*contfut_data['open_interest'].to_list()[0]
        data1 = data[(data.volume>=volume_thresh)&(data.open_interest>=oi_thresh)]
        data1 = data1[data1.symbol>contfut]
        # 返回日期最小的合约
        contracts = data1.symbol.to_list()
        if contracts == []:
            return []
        else:
            # 筛选出比confut日期大的合约里日期最小的合约, 返回表头顺序的list数据
            contfut_new = select_contfut(contracts)
            remarks = f"{today} switch {contfut} to {contfut_new}"
            new_close = data[data.symbol==contfut_new]['close_price'].to_list()[0]
            # 这里需要获取下一个交易日
            next_td_day = pd.to_datetime(str(get_next_trading_day(today)), format="%Y%m%d")
            today_dt = pd.to_datetime(str(today), format="%Y%m%d")
            symbol_ex_info = [next_td_day, vt_symbol, today_dt, None, None, contfut_close/new_close, today_dt, None, None, None, None, contfut_close - new_close, contfut_close, remarks]
            return symbol_ex_info

def insert_into_mysql_database(df, tablename, cols):
    _values = ','.join(['%s']*len(cols.split(',')))
    sql = f"insert into {tablename} ("+cols+") VALUES(%s)"%_values
    all_array = df.to_numpy()
    all_tuple = []
    count = 0
    for i in all_array:
        tmp = tuple(i)
        all_tuple.append(tmp)
        count += 1
        if count%5000 == 0:       # 每5000条存储一次
            cursor.executemany(sql, all_tuple)
            conn.commit()
            all_tuple = []
        
    # 将没有存完的数据存储好
    if count%5000 !=0:
        cursor.executemany(sql, all_tuple)
        conn.commit()
    return

def insert_into_mysql_database_by_list(df, tablename, cols):
    _values = ','.join(['%s']*len(cols.split(',')))
    sql = f"insert into {tablename} ("+cols+") VALUES(%s)"%_values
    all_array = df
    all_tuple = []
    count = 0
    for i in all_array:
        tmp = tuple(i)
        all_tuple.append(tmp)
        count += 1
        if count%5000 == 0:       # 每5000条存储一次
            cursor.executemany(sql, all_tuple)
            conn.commit()
            all_tuple = []
        
    # 将没有存完的数据存储好
    if count%5000 !=0:
        cursor.executemany(sql, all_tuple)
        conn.commit()
    return

def get_domiant(data):
    for i in range(len(data)):
        symbol = data.iloc[i]['underlying_symbol']
        dominant_contract = rqdatac.futures.get_dominant(symbol.upper(), '20221230').to_list()[0]
        data.loc[data.index[i], 'remarks'] = f'switch None to {dominant_contract}'
    return data

def get_vt_symbol(df):
    vt_symbol = symbol_cap2symbol[df+'888']+'.'+symbol_cap2exchange[df]
    # print(vt_symbol)
    return vt_symbol

def update_initial_data(sdate):
    # 初始化主力，使用米筐的主力，而不是用我们自己的逻辑
    data = rqdatac.futures.get_ex_factor(list(symbol_cap2exchange.keys()), '20200501',sdate,adjust_method='prev_close_ratio')
    data1 = rqdatac.futures.get_ex_factor(list(symbol_cap2exchange.keys()), '20200501',sdate,adjust_method='prev_close_spread')
    data['spread'] = data1['ex_factor']
    data['order_book_id'] = data['underlying_symbol'].apply(get_vt_symbol)
    data['split_coefficient_from'] = None
    data['split_coefficient_to'] = None
    data['book_closure_date'] = data.index
    data['cash'] = None
    data['round_lot'] = None
    data['close'] = None
    data['ex_factor_theory'] = data['ex_factor']
    data = data.reset_index()
    data = data.groupby('underlying_symbol').last().reset_index()
    data = get_domiant(data)
    data['create_date'] = pd.to_datetime(time.strftime("%Y-%m-%d"))
    # print(data)
    cols_new = 'ex_date,order_book_id,book_closure_date,ex_cum_factor,ex_end_date,ex_factor,ex_factor_theory,create_date,cash,round_lot,split_coefficient_from,split_coefficient_to,spread,close,remarks'
    tablename = 'common_info.ex_factor_futures'
    insert_into_mysql_database(data[cols_new.split(',')], tablename, cols_new)
    print("Finish inserting into mysql!")
    return

def update_singleday_data(date):
    # 分品种找出需要换月的主力合约并汇总到ex_data
    ex_data = []
    tradedt_data = get_daily_data(date)
    tradedt_data['comdty'] = tradedt_data['symbol'].str.replace(r'[0-9]', '')
    for symbolx in symbols:
        symbol = symbolx.upper()
        vt_symbol = symbol2vt_symbol[symbolx]
        data = tradedt_data[tradedt_data['comdty'] == symbol]
        if data.empty:
            print(f"{symbol}: data is not updated yet!")
        else:
            ex_info = check_contfut(data, symbol, date, vt_symbol)
            if ex_info:
                ex_data.append(ex_info)
    # 将换月数据存入mysql
    if ex_data:
        cols = ['ex_date','order_book_id','book_closure_date','ex_cum_factor','ex_end_date','ex_factor','create_date','cash','round_lot','split_coefficient_from','split_coefficient_to','spread','close','remarks']
        ex_df = pd.DataFrame(ex_data, columns = cols)
        tablename = 'common_info.ex_factor_futures'
        print(ex_df)
        insert_into_mysql_database_by_list(ex_data, tablename, ','.join(cols))

def get_trade_date(sdate, edate):
    sql = f"select date from common_info.trading_day where date>={sdate} and date<={edate}"
    data = pd.read_sql(sql,conn)
    trade_dts = data['date'].to_list()
    return trade_dts

def create_ex_factor_table_mysql():
    sql = "create table if not exists common_info.ex_factor_futures ( \
    id INT AUTO_INCREMENT PRIMARY KEY,\
    ex_date datetime,\
    order_book_id char(20),\
    book_closure_date date,\
    ex_cum_factor float,\
    ex_end_date  date,\
    ex_factor float,\
    ex_factor_theory float,\
    create_date date,\
    cash float,\
    round_lot Int,\
    split_coefficient_from float,\
    split_coefficient_to float,\
    spread float,\
    close float,\
    remarks char(50)\
    )\
    ENGINE = InnoDB\
    "
    cursor.execute(sql)
    return

def update_history_data(sdate, edate):
    # update_initial_data(sdate)
    dates = get_trade_date(sdate, edate)
    for date in dates:
        update_singleday_data(date)

def update_everyday():
    # 指定日期
    date = int(time.strftime("%Y%m%d"))
    date = 20231027
    update_singleday_data(date)


if __name__ == '__main__':
    create_ex_factor_table_mysql()
    print_date = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{print_date}: {__file__}")

    # update_history_data(20230101, 20231026)
    update_everyday()
    print(f"{__file__}: Finished all work!")
    
