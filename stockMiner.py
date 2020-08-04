import json
import sqlite3
import time
from ES_apriori_Tools import *
from ES_apriori_Tools import get_dayn_set, generate_iid_name
import sys

"""
EM-Apriori的实验代码和改进 by 冯郭晟
TID(int) : 天数的标志
IID(string) : 事件的标志 "ID-d-p" ID为股票代码 d为{0,1,2, ... ,w}中一个,表示发生的在时间窗口中的相对天数 p为{-1,1}中一个分别表示跌、稳、涨  

支持度：一个项集的支持度被定义为数据集中包含该项集的记录所占的比例

"""


class StockMiner:
    def __init__(self,
                 min_sup,  # 最小支持度阈值 ------------- 经过多次实验我的最佳阈值为0.13
                 min_conf,  # 最小置信度阈值
                 window_size=3,  # 滑动窗口大小
                 max_l_num=4):  # 最大项集项的个数
        self.window_size = window_size
        self.min_sup = min_sup
        self.min_conf = min_conf
        self.max_l_items = max_l_num

    def generate_database(self, stock_path, origin_table_name, table_name, threshold=1, override=False):
        """
        根据清洗过数据的数据库表生成挖掘数据库表,存储在stock_path对应的数据库中
        :param override:是否覆盖原来的表
        :param origin_table_name: 清洗过数据的数据库表
        :param table_name: 生成的挖掘数据库表的名称
        :param threshold: 股票涨或跌的阈值，默认1%
        :param stock_path: 挖掘数据库地址
        :return: None
        """
        conn = sqlite3.connect(stock_path)
        print("Opened database successfully")
        c = conn.cursor()

        if override:  # 覆盖同名的表,防止表无法创建
            c.execute("DROP TABLE {0}".format(table_name))
        sql = '''CREATE TABLE {0} 
        (
        TID INT PRIMARY KEY NOT NULL,
        IID TEXT NOT NULL)'''.format(table_name)
        c.execute(sql)
        print("Table {0} created successfully in {1}".format(table_name, stock_path))
        selected_rows = c.execute("SELECT id, trans FROM {0} ".format(origin_table_name)).fetchall()
        T = len(json.loads(selected_rows[0][1]))  # 天数，也就是TID的范围
        print(T)
        for TID_count in range(T - self.window_size + 1):
            IID = []
            for stock in selected_rows:
                stock_history_data = json.loads(stock[1])
                for j in range(TID_count, TID_count + self.window_size):
                    # 事件的标志 "ID-d-p"
                    up_down = float(stock_history_data[j][2].replace('%', ''))
                    if up_down > threshold:
                        up_down = 1
                    elif up_down < -threshold:
                        up_down = -1
                    else:
                        continue  # 跳过没有涨跌的项
                    iid = "{0}:{1}:{2}".format(stock[0],  # 股票代码
                                               j - TID_count,  # 天数
                                               up_down, )  # 涨-稳-跌

                    IID.append(iid)

            c.execute('''INSERT INTO {0} (TID,IID) 
                      VALUES ({1},'{2}')'''.
                      format(table_name, TID_count, json.dumps(IID)))

            TID_count += 1
            print(TID_count)
        conn.commit()
        conn.close()

    def get_l1(self, stock_path, table_name):
        """
        读挖掘数据库，生成用于算法挖掘的L1倒排表
        仅在此读一遍数据库就够了，体现算法改进
        :param stock_path:数据库地址
        :param table_name:对应的表名字
        :return: C1倒排表和总的TID数量
        """
        conn = sqlite3.connect(stock_path)
        c = conn.cursor()
        database = c.execute('''SELECT * FROM {0}'''.format(table_name)).fetchall()
        # print('数据库大小为 {0} B'.format(sys.getsizeof(database)))
        C1 = {}
        TID_num = 0

        start_l1 = time.time()
        for item in database:
            TID_num += 1
            IID = json.loads(item[1])
            for iid in IID:
                if iid not in C1:
                    C1[iid] = set()
                C1[iid].add(item[0])

        # 删除置信度小于最小置信度的项
        discard_set = []
        for IID in C1:
            if len(C1[IID]) / TID_num < self.min_sup:
                discard_set.append(IID)
        for discard_item in discard_set:
            C1.pop(discard_item)
        end_l1 = time.time()
        print('生成L1倒排表成功，满足最小置信度共 {0} 项,丢弃了 {1} 项'.format(len(C1), len(discard_set)))
        print(f'生成L1倒排表所用时间为{(end_l1 - start_l1)}s')
        return C1, TID_num

    def es_apriori(self, l1, TID_num, mine_num=None, verbose=False, evaluateALL=False):
        """
        ES-Apriori挖掘算法改进实现
        挖掘所有的规则可以分成u步运行：每步挖掘只包含ei(0), 1<=i<=u 的关联规则
        :param evaluateALL: 估测挖掘全部L2项集的元素数量（不精准）
        :param mine_num: 挖掘的l1项集中的元素数量，根据ES-Apriori的特性，可以不用全部挖掘
        :param verbose: 是否输出显示挖掘的具体情况
        :param l1: 扫描一次（仅一次）数据库并进行处理得到的L1倒排表
        :return: rules 挖掘出的规则，按三元组来存储
        """
        close_set = set()  # 存储已经挖掘过的股票的id，避免重复挖掘，以集合的形式记录
        rules = []
        l1_len = len(l1)
        if mine_num is None:
            mine_num = l1_len
        print('总共有{0}个待挖掘的基准项'.format(l1_len))
        l1_count = 0  # 记录当前挖掘到第几项
        begin_time = time.time()
        size_ls = []
        for key in l1:  # 每次只挖掘一个基准项
            size_l = 0

            print('当前挖掘到第 {0} 项，花费时间为 {1} s，挖掘进度为 {2} %'.
                  format(l1_count, round(time.time() - begin_time, 2), round(l1_count / mine_num, 3)*100))
            l1_count += 1  # 挖掘的项数统计
            if l1_count >= mine_num:
                return size_ls
                # return rules
            l = []  # 倒排表的集合
            l.append(l1)
            l_count = 0  # 实际上倒排表可以迭代生成，这里count表示生成的是l_count倒排表
            assert isinstance(key, str)
            key_split = key.split(':')
            day = int(key_split[1])
            if day != 0:  # 所有规则都可以在各时间序列观察值的参考时间基准项ei(0)的基础上产生
                continue

            # 生成2项集,也就是L2倒排表
            start_l2 = time.time()
            l2 = {}
            discard_count = 0
            for key2 in l1:
                assert isinstance(key2, str)
                if key2 == key:
                    continue
                else:
                    l2_key = generate_iid_name({key, key2})
                    if l2_key in close_set:
                        continue
                    else:
                        close_set.add(l2_key)
                    l2[l2_key] = set()
                    for TID in l1[key]:
                        if TID in l1[key2]:
                            l2[l2_key].add(TID)
                    if len(l2[l2_key]) / TID_num < self.min_sup:
                        l2.pop(l2_key)  # 删除支持度小于最小支持度的项
                        discard_count += 1

            l_count += 1
            for key in l2.keys():
                size_l += sys.getsizeof(l2[key])
            l.append(l2)
            end_l2 = time.time()
            if verbose:
                print("生成L2倒排表成功，满足最小置信度共 {0} 项，丢弃了 {1} 项".format(len(l[l_count]), discard_count))
                if evaluateALL:
                    print(
                        f"估计生成全部L{l_count + 1}倒排表满足最小置信度共 {len(l[l_count]) * len(l1) / 15} 项，共丢弃了 {discard_count * len(l1) / 4} 项")
                    print(f'估计生成全部L2倒排表所用时间为{(end_l2 - start_l2) * len(l1)}s')

            # 迭代生成Li倒排表
            for i in range(l_count, self.max_l_items - 1):
                l_new = {}
                discard_count = 0
                start_time = time.time()
                count = 0
                iid_old = list(l[l_count])
                for x in range(0, len(iid_old)):
                    key_old1 = iid_old[x]
                    for y in range(x + 1, len(iid_old)):
                        key_old2 = iid_old[y]
                        IID1 = set(key_old1.split('&'))
                        IID2 = set(key_old2.split('&'))
                        new_IID = IID1 | IID2
                        if len(new_IID) != l_count + 2:
                            continue
                        new_key = generate_iid_name(new_IID)
                        if new_key in close_set:
                            continue
                        else:
                            close_set.add(new_key)
                        l_new[new_key] = set()
                        l_new[new_key] = l[l_count][key_old1] & l[l_count][key_old2]  # 求交
                        if len(l_new[new_key]) / TID_num < self.min_sup:
                            l_new.pop(new_key)  # 删除支持度小于最小支持度的项
                            discard_count += 1
                    count += 1
                    if count % 1000 == 0 and verbose:
                        print(count)
                        print(time.time() - start_time)
                if not len(l_new) == 0:
                    l_count += 1
                    l.append(l_new)
                    if verbose:
                        print(
                            "生成L{0}倒排表成功，满足最小支持度共 {1} 项，丢弃了 {2} 项".format(l_count + 1, len(l[l_count]), discard_count))
                        if len(l[l_count]) > 30000:
                            print("计算{0}项共需要时间为".format(len(l[l_count]), round(time.time() - start_time), 2))
                    for key in l_new.keys():
                        size_l += sys.getsizeof(l_new[key])
                    del l_new  # 清理内存

                else:
                    if verbose:
                        print("至第L{0}项已经没有满足支持度要求的项集,算法提前返回!".format(l_count + 1))
            if verbose:
                print("各个Ln倒排表生成成功，开始挖掘关联规则")
            print(f'空间消耗为{size_l}bit')
            size_ls.append(size_l)
            # rules += miningRules(l, self.min_conf, self.window_size, dump_bests=True)  # 挖掘关联规则
        return size_ls
        # return rules


def miningRules(l, min_conf, w, dump_bests=False):
    """
    利用生成的满足支持度的频繁项集合构造的Ln倒排表来按照ES-Apriori规则挖掘时序关联规则
    :param l: 生成的满足支持度的频繁项集合构造的Ln倒排表
    :return:
    """
    max_l_num = len(l)
    rules = []
    for l_index in range(max_l_num - 1, 0, -1):
        ln = l[l_index]
        for iid in ln:  # ln是l_index对应的倒排表 如l_index=4就是L5倒排表
            assert isinstance(iid, str)
            IID = set(iid.split('&'))
            dayn_set = get_dayn_set(iid, w)
            max_day_num = len(dayn_set)
            X = set()
            for day_index in range(0, max_day_num - 1):  # day0为前件推day1,day2，day0,day1为前件推day2...一直到dayw-1
                if len(dayn_set[day_index]) == 0:  # 对应天数没有对应的项，如a0b2a3没有第1天的项
                    continue
                X = dayn_set[day_index] | X  # 取出第day_index天的iid项集作为前件，注意前件要递增，a1出现则a0一定要有
                X_n = len(X) - 1  # 前件的长度-1，对应ln n=X_n的倒排表的索引

                confidence = len(ln[iid]) / len(l[X_n][generate_iid_name(X)])  # 置信度的计算，分母都是总天数，被消去

                if confidence < min_conf:
                    continue
                if dump_bests:  # 将满足置信度的关联关系的前件和后件的出现天数导出到json文件
                    bests = {}
                    if confidence > 0.50:
                        bests[f'conf'] = confidence
                        bests[f'Y : {iid}'] = list(ln[iid])
                        bests[f'X : {generate_iid_name(X)}'] = list(l[X_n][generate_iid_name(X)])
                        with open('./data/bests.json', 'a') as f:
                            print('导出高置信度项!')
                            print(bests)
                            json.dump(bests, f)
                Y = IID - X  # 组合项的集合减去前件得到后件
                rules.append((generate_iid_name(X), generate_iid_name(Y), confidence))
                if len(X) > 1:
                    print("{0} -> {1} 置信度:{2}".format(generate_iid_name(X), generate_iid_name(Y), confidence))
    return rules
