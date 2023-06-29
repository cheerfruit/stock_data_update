各脚本功能说明: 

# clickhouse：
insert_into_ck: 此脚本用于插入无复权价格行情和复权因子, 在建库和新加股票池时运行
cal_adjprice_and_insert_into_ck.py: 此脚本用于插入所有股票的前复权数据(自己根据复权因子计算), 建库时或者增加票池时跑,
update_adjprice_into_ck.py: 用于股票前复权价每日增量插入clickhouse库(备用), 需要每天跑
trading_info_data_to_ck.py: 用于插入一些通用的数据信息，比如合约信息，历史所有交易日等等

# mysql：
insert_preadj_price_into_mysql.py: 此脚本用于插入所有股票的前复权数据(直接从米筐下载), 建库时或者增加票池时跑
update_preadj_price_into_mysql.py: 用于股票前复权价(米筐下载)每日增量插入mysql库, 需要每天跑，因为每天8:00 am后米筐更新前复权价, 所以此脚本在8:00 am时更新数据

# check database
check_database_history.py: 此脚本用于检查数据库历史数据是否有问题.
check_database_everyday.py: 此脚本用于检查数据库当天最新数据是否有问题.
