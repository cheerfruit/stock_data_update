from vnpy.trader.constant import Exchange, Interval

all_sizes = {'a': 10, 'ag': 15, 'al': 5, 'AP': 10, 'au': 1000, 'b': 10, 'bb': 500, 'BC': 5, 'bu': 10, 'c': 10,
                'CF': 5,
                'CJ': 5, 'cs': 10, 'cu': 5, 'CY': 5, 'eb': 5, 'eg': 10, 'fb': 10, 'FG': 20, 'fu': 10, 'hc': 10,
                'i': 100,
                'j': 100, 'jd': 10, 'jm': 60, 'JR': 20, 'l': 5, 'lh': 16, 'LR': 20, 'LU': 10, 'm': 10, 'MA': 10,
                'ni': 1,
                'NR': 10, 'OI': 10, 'p': 10, 'pb': 5, 'PF': 5, 'pg': 20, 'PK': 5, 'PM': 50, 'pp': 5, 'rb': 10,
                'RI': 20,
                'RM': 10, 'rr': 10, 'RS': 10, 'ru': 10, 'SA': 20, 'SC': 1000, 'SF': 5, 'SM': 5, 'sn': 1, 'sp': 10,
                'SR': 10,
                'ss': 5, 'TA': 5, 'UR': 20, 'v': 5, 'WH': 20, 'wr': 10, 'y': 10, 'ZC': 100, 'zn': 5, 'IC': 200,
                'IF': 300,
                'IH': 300, 'IM': 200, 'T': 10000, 'TF': 10000, 'TS': 20000,
                'LC': 1, 'SI':5
                }
all_symbol_pres = {
    'DCE': ['a', 'b', 'bb', 'c', 'cs', 'eb', 'eg', 'fb', 'i', 'j', 'jd', 'jm', 'l', 'lh', 'm', 'p', 'pg', 'pp',
            'rr', 'v', 'y'],
    'SHFE': ['ag', 'al', 'au', 'bu', 'cu', 'fu', 'hc', 'ni', 'pb', 'rb', 'ru', 'sn', 'sp', 'ss', 'wr', 'zn'],
    'CZCE': ['AP', 'CF', 'CJ', 'CY', 'FG', 'JR', 'LR', 'MA', 'OI', 'PF', 'PK', 'PM', 'RI', 'RM', 'RS', 'SA', 'SF',
                'SM', 'SR', 'TA', 'UR', 'WH', 'ZC'], 'INE': ['BC', 'LU', 'NR', 'SC'],
    'CFFEX': ['IC', 'IF', 'IH', 'IM', 'T', 'TF', 'TS'],
    'GFEX': ['LC','SI']}

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
