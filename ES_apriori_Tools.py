def generate_iid_name(IID_set):
    """
    生成满足一定规则的IID名字，方便后面挖掘规则的需要
    :param IID_set:
    :return:
    """
    import functools
    if not isinstance(IID_set, list):
        IID_set = list(IID_set)
    IID_set.sort(key=functools.cmp_to_key(compare_iid))
    return '&'.join(IID_set)


def compare_iid(iid1, iid2):
    assert isinstance(iid1, str)
    assert isinstance(iid2, str)
    iid1_arr = iid1.split(':')
    iid2_arr = iid2.split(':')
    iid1_count = int(iid1_arr[0])+0.1*int(iid1_arr[1])+0.01*int(iid1_arr[2])
    iid2_count = int(iid2_arr[0]) + 0.1 * int(iid2_arr[1]) + 0.01 * int(iid2_arr[2])
    if iid1_count > iid2_count:
        return 1
    elif iid1_count == iid2_count:
        return 0
    else:
        return -1


def get_dayn_set(iid, w):
    """
    iid，获取项中各个事件对应的天数，并放到各自的集合里面
    :param iid:  倒排表中的iid项
    :param w: window size
    :return:
    """
    dayn_sets = []
    IID = iid.split('&')
    max_day_num = 0

    # 看这个项中包含了最晚是第几天的数据 0<max_day_num<w
    for iid in IID:
        day_num = get_iid_day(iid)
        if day_num > max_day_num:
            max_day_num = day_num

    for i in range(0, max_day_num+1):
        dayn_sets.append(set())
    for split_iid in IID:
        try:
            dayn_sets[get_iid_day(split_iid)].add(split_iid)
        except:
            print("{0} {1} {2}".format(len(dayn_sets), get_iid_day(split_iid), split_iid))
    return dayn_sets


def get_iid_day(iid):
    """
    解析iid项获取对应的天数
    :param iid:
    :return:
    """
    assert isinstance(iid, str)
    return int(iid.split(':')[1])


def get_iid_id(iid):
    """
    解析iid项获取对应的天数
    :param iid:
    :return:
    """
    assert isinstance(iid, str)
    return iid.split(':')[0]


def process_rules(rules, db_path, table_name, id2name_table, override=False):
    import sqlite3
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    if override:
        c.execute("DROP TABLE {0}".format(table_name))
    sql = '''CREATE TABLE {0} 
        (
        X TEXT NOT NULL,
        Y TEXT NOT NULL,
        Conf TEXT NOT NULL)'''.format(table_name)
    c.execute(sql)
    for rule in rules:  # rule是(x,y,conf)三元组形式
        x = rule[0]  # 规则前件
        y = rule[1]  # 规则后件
        conf = round(float(rule[2]), 2)
        assert isinstance(x, str) and isinstance(y, str)
        X = x.split('&')
        Y = y.split('&')
        X_len = len(X)
        Y_len = len(Y)
        for i in range(0,X_len):
            X[i] = get_semantic_rule(X[i], id2name_table)
        for j in range(0,Y_len):
            Y[j] = get_semantic_rule(Y[j], id2name_table)
        final_x = ',同时'.join(X)
        final_y = ',同时'.join(Y)
        c.execute('''INSERT INTO {0} (X,Y,conf) 
        VALUES ('{1}','{2}','{3}')'''.format(table_name, final_x, final_y, str(conf)) )
    conn.commit()
    conn.close()


def get_semantic_rule(origin_rule, id2name_table):
    assert isinstance(origin_rule, str)
    origin_rule = origin_rule.split(':')
    id = int(origin_rule[0])
    name = id2name_table[id]
    day = int(origin_rule[1])+1
    up_down = "上涨" if int(origin_rule[2]) == 1 else "下跌"
    return "{0}在第{1}天{2}".format(name,day,up_down)


def generate_id2name_table(db_path, table_name):
    """
    根据原始数据生成股票id和名字对应的表
    :param db_path:
    :param table_name:
    :return:
    """
    id2name_table = {}
    import sqlite3
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    sql = '''SELECT id,name FROM {0}'''.format(table_name)
    selected_rows = c.execute(sql)
    for id_name in selected_rows:
        id2name_table[id_name[0]] = id_name[1]
    return id2name_table

def plot_rela():
    import sqlite3
    import matplotlib.pyplot as plt
    import json
    import matplotlib
    conn = sqlite3.connect('./data/my_stock_data.db')
    c = conn.cursor()
    selected_stock = c.execute('''SELECT * from StockData_Lab_ana WHERE id == 2234 OR  id == 2746 OR id == 150288''').fetchall()
    name = []
    prices = []
    X = range(len(json.loads(selected_stock[0][2])))
    for stock in range(len(selected_stock)):
        name.append(selected_stock[stock][1])
        price_arr = []
        trans = json.loads(selected_stock[stock][2])
        for tran in trans:
            if stock == 2:
                price_arr.append(float(tran[2])*20)
            else:
                price_arr.append(float(tran[2]))
        prices.append(price_arr)

    for price in prices:
        plt.plot(X, price)
    matplotlib.rcParams['font.family'] = 'SimHei'
    plt.xlabel('day')
    plt.ylabel('price')
    plt.legend(name)

    # verticalLine = [259, 132, 388, 391, 136, 137, 268, 398, 16, 144, 152, 153, 409, 411, 285, 159, 288, 34, 292, 425, 43, 303, 432, 433, 50, 437, 311, 185, 58, 443, 317, 446, 447, 193, 199, 327, 203, 77, 207, 83, 211, 468, 470, 343, 89, 94, 480, 483, 356, 102, 359, 233, 234, 492, 493, 240, 113, 242, 373, 246, 123, 381]
    # for x in verticalLine:
    #     plt.axvline(x=x, ls="--", c="green")
    # plt.show()