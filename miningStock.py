from stockMiner import *
from bitmap_stockMiner import *
import json

stockMiner = StockMiner(min_sup=0.13, min_conf=0.3)
# stockMiner.generate_database("./data/my_stock_data.db", "StockData_Lab", "StockData_mine_find", override=True) # 下一次用的时候记得创建一个新表
l1, TID_num = stockMiner.get_l1("./data/my_stock_data.db", "StockData_mine")
rules = stockMiner.es_apriori(l1=l1, TID_num=TID_num, mine_num=500, verbose=True, evaluateALL=False)
process_rules(rules, './data/rules.db', 'rules1',
              generate_id2name_table("./data/my_stock_data.db", "StockData_Lab"),
              override=True)