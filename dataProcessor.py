import sqlite3
import json

"""
这个是用来清洗原来的爬下来的数据的类，包括股票交易数据日期对齐，方便后面的时间窗口的运算
"""


class OriginDataProcessor:
    def __init__(self, database_path, my_database_path):
        self.database_path = database_path
        self.my_database_path = my_database_path

    # TODO 按类别来过滤数据
    def filter_data_by_category(self):
        None

    def filter_data_by_range(self, table_name, start_code, end_code, days, item_num=500, overide=False):
        """
        根据已有的股票表中的数据选取需要的数量的股票数据生成新的表
        注意数据一开始是不对齐的
        :param item_num:
        :param start_code: 开始选择的股票编号
        :param end_code: 停止选择的股票编号
        :param table_name: 新创建的表的名字
        :return: none
        """
        conn = sqlite3.connect(self.database_path)
        my_conn = sqlite3.connect(self.my_database_path)  # 连接到过滤后数据的数据库,没有则自动创建一个
        print("Opened database successfully")
        c = conn.cursor()
        c1 = conn.cursor()
        my_c = my_conn.cursor()
        if overide:  # 覆盖同名的表
            my_c.execute("DROP TABLE {0}".format(table_name))
        sql = '''CREATE TABLE {0} 
        (
        id INT PRIMARY KEY NOT NULL,
        name TEXT NOT NULL,
        trans TEXT NOT NULL)'''.format(table_name)
        my_c.execute(sql)

        print("Table {0} created successfully".format(table_name))

        selected_rows = c.execute("SELECT Code, HisText FROM StockHisText "
                                  "WHERE Code BETWEEN {0} AND {1}".format(start_code, end_code))
        selected_rows = selected_rows.fetchall()

        selected_name_rows = c1.execute("SELECT Code, Name FROM StockBaseInfo "
                                        "WHERE Code BETWEEN {0} AND {1}".format(start_code, end_code))

        selected_name_rows = selected_name_rows.fetchall()
        insert_count = 0  # 记录插入了多少条数据
        for i in range(len(selected_rows)):
            code = selected_rows[i][0]
            assert selected_name_rows[i][0] == code and code <= end_code

            history_text = selected_rows[i][1]
            history_text_json = json.loads(history_text)

            if history_text_json[0]["status"] == 2:
                continue  # 如果是不存在的要过滤掉
            if history_text_json[0]["hq"][0][0] != "2018-02-09":
                continue  # 跳过不是2018-02-09结束的记录

            # 截取days天的交易数据
            if len(history_text_json[0]["hq"]) < days:
                continue
            else:
                history_text_json[0]["hq"] = history_text_json[0]["hq"][:days]

            if history_text_json[0]["hq"][-1][0] != "2016-01-20":
                continue
            history_text_json[0]["hq"] = history_text_json[0]["hq"][::-1]
            for k in range(len(history_text_json[0]["hq"])):
                origin_item = history_text_json[0]["hq"][k]
                new_item = [k, origin_item[0], origin_item[2]]
                history_text_json[0]["hq"][k] = new_item
            # for k in range(len(history_text_json[0]["hq"])):
            #     origin_item = history_text_json[0]["hq"][k]
            #     new_item = [k, origin_item[0], origin_item[4]]
            #     history_text_json[0]["hq"][k] = new_item
            history_text = json.dumps(history_text_json[0]["hq"])
            # history_text = json.dumps(history_text_json)
            name = selected_name_rows[i][1]
            if code == 131805 or code == 131801:  # 跳过两支特殊股票
                continue
            sql = "INSERT INTO {3} (id,name,trans)" \
                  "VALUES ({0},'{1}','{2}')".format(code, name, history_text, table_name)
            my_c.execute(sql)
            insert_count += 1
            if insert_count >= item_num:  # 选够了需要的item_num个记录则结束filter
                break
        print("successfully inserted {0} items at table{1} at {2}".
              format(str(insert_count), table_name, self.my_database_path))
        conn.commit()
        my_conn.commit()
        conn.close()
        my_conn.close()


dataProcess = OriginDataProcessor("./data/stock_his.db", "./data/my_stock_data.db")
dataProcess.filter_data_by_range("StockData_Lab_ana", start_code=1, end_code=600000, days=500, overide=False)
