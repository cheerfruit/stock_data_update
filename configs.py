from vnpy.trader.constant import Exchange, Interval

all_sizes = {'a': 10, 'ag': 15, 'al': 5, 'ao': 20, 'AP': 10, 'au': 1000, 'b': 10, 'bb': 500, 'bc': 5, 'br': 5, 'bu': 10,
             'c': 10, 'CF': 5, 'CJ': 5, 'cs': 10, 'cu': 5, 'CY': 5, 'eb': 5, 'ec': 50, 'eg': 10, 'fb': 10, 'FG': 20,
             'fu': 10, 'hc': 10, 'i': 100, 'j': 100, 'jd': 10, 'jm': 60, 'JR': 20, 'l': 5, 'lc': 1, 'lh': 16, 'LR': 20,
             'lu': 10, 'm': 10, 'MA': 10, 'ni': 1, 'nr': 10, 'OI': 10, 'p': 10, 'pb': 5, 'PF': 5, 'pg': 20, 'PK': 5,
             'PM': 50, 'pp': 5, 'PX': 5, 'rb': 10, 'RI': 20, 'RM': 10, 'rr': 10, 'RS': 10, 'ru': 10, 'SA': 20,
             'sc': 1000, 'SF': 5, 'SH': 30, 'si': 5, 'SM': 5, 'sn': 1, 'sp': 10, 'SR': 10, 'ss': 5,
             'TA': 5, 'UR': 20, 'v': 5, 'WH': 20, 'wr': 10, 'y': 10, 'ZC': 100, 'zn': 5, 'IC': 200, 'IF': 300,
             'IH': 300, 'IM': 200, 'T': 10000, 'TF': 10000, 'TS': 20000}

all_priceticks = {'a': 1.0, 'ag': 1.0, 'al': 5.0, 'ao': 1.0, 'AP': 1.0, 'au': 0.02, 'b': 1.0, 'bb': 0.05, 'bc': 10.0,
                  'br': 5.0, 'bu': 1.0, 'c': 1.0, 'CF': 5.0, 'CJ': 5.0, 'cs': 1.0, 'cu': 10.0, 'CY': 5.0, 'eb': 1.0,
                  'ec': 0.1, 'eg': 1.0, 'fb': 0.5, 'FG': 1.0, 'fu': 1.0, 'hc': 1.0, 'i': 0.5, 'j': 0.5, 'jd': 1.0,
                  'jm': 0.5, 'JR': 1.0, 'l': 1.0, 'lc': 50.0, 'lh': 5.0, 'LR': 1.0, 'lu': 1.0, 'm': 1.0, 'MA': 1.0,
                  'ni': 10.0, 'nr': 5.0, 'OI': 1.0, 'p': 2.0, 'pb': 5.0, 'PF': 2.0, 'pg': 1.0, 'PK': 2.0, 'PM': 1.0,
                  'pp': 1.0, 'PX': 2.0, 'rb': 1.0, 'RI': 1.0, 'RM': 1.0, 'rr': 1.0, 'RS': 1.0, 'ru': 5.0, 'SA': 1.0,
                  'sc': 0.1, 'SF': 2.0, 'SH': 1.0, 'si': 5.0, 'SM': 2.0, 'sn': 10.0, 'sp': 2.0, 'SR': 1.0,
                  'ss': 5.0, 'TA': 2.0, 'UR': 1.0, 'v': 1.0, 'WH': 1.0, 'wr': 1.0, 'y': 2.0, 'ZC': 0.2, 'zn': 5.0,
                  'IC': 0.2, 'IF': 0.2, 'IH': 0.2, 'IM': 0.2, 'T': 0.005, 'TF': 0.005, 'TS': 0.002}

all_symbol_pres = {
    'DCE': ['a', 'b', 'bb', 'c', 'cs', 'eb', 'eg', 'fb', 'i', 'j', 'jd', 'jm', 'l', 'lh', 'm', 'p', 'pg', 'pp', 'rr',
            'v', 'y'],
    'SHFE': ['ag', 'al', 'ao', 'au', 'br', 'bu', 'cu', 'fu', 'hc', 'ni', 'pb', 'rb', 'ru', 'sn', 'sp', 'ss', 'wr',
             'zn'],
    'CZCE': ['AP', 'CF', 'CJ', 'CY', 'FG', 'JR', 'LR', 'MA', 'OI', 'PF', 'PK', 'PM', 'PX', 'RI', 'RM', 'RS', 'SA', 'SF',
             'SH', 'SM', 'SR', 'TA', 'UR', 'WH', 'ZC'], 'INE': ['bc', 'ec', 'lu', 'nr', 'sc'],
    'GFEX': ['lc', 'si'], 'CFFEX': ['IC', 'IF', 'IH', 'IM', 'T', 'TF', 'TS']}

all_symbols = ['a', 'b', 'bb', 'c', 'cs', 'eb', 'eg', 'fb', 'i', 'j', 'jd', 'jm', 'l', 'lh', 'm', 'p', 'pg', 'pp', 'rr',
               'v', 'y', 'ag', 'al', 'ao', 'au', 'br', 'bu', 'cu', 'fu', 'hc', 'ni', 'pb', 'rb', 'ru', 'sn', 'sp', 'ss',
               'wr', 'zn', 'AP', 'CF', 'CJ', 'CY', 'FG', 'JR', 'LR', 'MA', 'OI', 'PF', 'PK', 'PM', 'PX', 'RI', 'RM',
               'RS', 'SA', 'SF', 'SH', 'SM', 'SR', 'TA', 'UR', 'WH', 'ZC', 'bc', 'ec', 'lu', 'nr', 'sc', 'lc',
               'si', 'IC', 'IF', 'IH', 'IM', 'T', 'TF', 'TS']

dbsymbols = ['a', 'ag', 'al', 'AP', 'au', 'b', 'bb', 'bc', 'bu', 'c', 'CF', 'CJ', 'cs', 'cu', 'CY', 'eb', 'eg', 'fb',
             'FG', 'fu', 'hc', 'i', 'IC', 'IF', 'IH', 'IM', 'j', 'jd', 'jm', 'JR', 'l', 'lc', 'lh', 'LR', 'lu', 'm',
             'MA', 'ni', 'nr', 'OI', 'p', 'pb', 'PF', 'pg', 'PK', 'PM', 'pp', 'rb', 'RI', 'RM', 'rr', 'RS', 'ru', 'SA',
             'sc', 'SF', 'si', 'SM', 'sn', 'sp', 'SR', 'ss', 'T', 'TA', 'TF', 'TS', 'UR', 'v', 'WH', 'wr', 'y', 'ZC',
             'zn']

# 打印all_symbols - dbsymbols
# print(set(all_symbols) - set(dbsymbols))

trading_hours = {
    0: ['AP', 'CJ', 'JR', 'LR', 'PK', 'PM', 'RI', 'RS', 'SF', 'SM', 'UR', 'WH', 'bb', 'ec', 'fb', 'jd', 'lc', 'lh',
        'si', 'wr'],# 0：白盘品种
    1: ['CF', 'CY', 'FG', 'MA', 'OI', 'PF', 'PX', 'RM', 'SA', 'SH', 'SR', 'TA', 'ZC', 'a', 'b', 'br', 'bu', 'c', 'cs',
        'eb', 'eg', 'fu', 'hc', 'i', 'j', 'jm', 'l', 'lu', 'm', 'nr', 'p', 'pg', 'pp', 'rb', 'rr', 'ru', 'sp', 'v',
        'y'],# 1：夜盘到23点品种
    2: ['al', 'ao', 'bc', 'cu', 'ni', 'pb', 'sn', 'sn', 'ss', 'zn'],# 2：夜盘到凌晨1点品种
    3: ['ag', 'au', 'sc'],# 3：夜盘到凌晨2点30分品种
    4: ['T', 'TF', 'TS'],# 4：9:30-11:30,13:00-15:15
    5: ['IC', 'IF', 'IH', 'IM'],# 5：9:30-11:30,13:00-15:00
}


continues_symbol = ['al', 'cu', 'ni', 'pb', 'sn', 'ss','zn','IC', 'IF', 'IM', 'IH', 'eb','PF']

symbols = all_sizes.keys()

# 将symbols转换为vt_symbols，如rb转换为rb888.SHFE，其中SHFE从all_symbol_pres中获取
vt_symbols = [f"{s}888.{exchange}" for s in symbols for exchange in all_symbol_pres if
                s in all_symbol_pres[exchange]]
symbol2vt_symbol = {s:f"{s}888.{exchange}" for s in symbols for exchange in all_symbol_pres if
                s in all_symbol_pres[exchange]}
rq_symbols = [f"{s}888" for s in symbols]
symbol_cap2exchange = {s.upper():f"{exchange}" for s in symbols for exchange in all_symbol_pres if
                s in all_symbol_pres[exchange]}
symbol_cap2symbol = {(s.upper()+'888'):f"{s}888" for s in symbols}

exchg_dict = {"SSE":Exchange.SSE, \
            "SZSE":Exchange.SZSE,\
            "CFFEX":Exchange.CFFEX, \
            "SHFE": Exchange.SHFE, \
            "CZCE":Exchange.CZCE, \
            "DCE":Exchange.DCE, \
            "INE":Exchange.INE, \
            "GFEX":Exchange.GFEX
            }
interval_dict = {'1m':Interval.MINUTE,
                 '1h':Interval.HOUR,
                 'd':Interval.DAILY,
                 'w':Interval.WEEKLY
                 }
