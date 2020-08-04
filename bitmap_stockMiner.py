"""
采用bitmap 算法改进的ES-Apriori算法 by 冯郭晟
"""
import json
import sqlite3
import time
from ES_apriori_Tools import get_dayn_set, generate_iid_name
import sys
from bitarray import bitarray
from itertools import product


class bitmap_stock_miner:
    def __init__(self,
                 min_sup,  # 最小支持度阈值 ------------- 经过多次实验我的最佳阈值为0.13
                 min_conf,  # 最小置信度阈值
                 window_size=3,  # 滑动窗口大小
                 max_l_num=4):  # 最大项集项的个数
        self.window_size = window_size
        self.min_sup = min_sup
        self.min_conf = min_conf
        self.max_l_items = max_l_num

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
                    C1[iid] = bitarray(500)
                    C1[iid].setall(False)
                C1[iid][int(item[0])-1] = True

        # 删除置信度小于最小置信度的项
        discard_set = []
        for iid in C1:
            if C1[iid].count(1) / TID_num < self.min_sup:
                discard_set.append(iid)
        for discard_item in discard_set:
            C1.pop(discard_item)
        end_l1 = time.time()
        print('生成L1倒排表成功，满足最小置信度共 {0} 项,丢弃了 {1} 项'.format(len(C1), len(discard_set)))
        print(f'生成L1倒排表所用时间为{(end_l1 - start_l1)}s')
        return C1, TID_num

    def es_apriori(self, l1, TID_num, mine_num=None, verbose=False):
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
        for iid in l1:  # 每次只挖掘一个基准项
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
            assert isinstance(iid, str)
            key_split = iid.split(':')
            day = int(key_split[1])
            if day != 0:  # 所有规则都可以在各时间序列观察值的参考时间基准项ei(0)的基础上产生
                continue

            # 生成2项集,也就是L2倒排表
            start_l2 = time.time()
            l2 = {}
            discard_count = 0
            
            for _iid in l1:
                assert isinstance(_iid, str)
                if _iid == iid:
                    continue
                else:
                    l2_key = generate_iid_name({iid, _iid})
                    if l2_key in close_set:
                        continue
                    else:
                        close_set.add(l2_key)
                    l2[l2_key] = bitarray(500)
                    l2[l2_key] = l1[iid] & l1[_iid]
                    # for TID in l1[iid]:
                    #     if TID in l1[_iid]:
                    #         l2[l2_key].add(TID)
                    if l2[l2_key].count(1) / TID_num < self.min_sup:
                        l2.pop(l2_key)  # 删除支持度小于最小支持度的项
                        discard_count += 1

            l_count += 1
            l.append(l2)
            for key in l2.keys():
                size_l += sys.getsizeof(l2[key])
            end_l2 = time.time()
            if verbose:
                print("生成L2倒排表成功，满足最小置信度共 {0} 项，丢弃了 {1} 项".format(len(l[l_count]), discard_count))

            # 迭代生成Li倒排表
            for i in range(l_count, self.max_l_items - 1):
                l_new = {}
                discard_count = 0
                start_time = time.time()
                count = 0
                iid_old = list(l[l_count])
                print(f'len = {len(iid_old)}')
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
                        l_new[new_key] = bitarray(500)
                        l_new[new_key] = l[l_count][key_old1] & l[l_count][key_old2]  # 求交
                        if l_new[new_key].count(1) / TID_num < self.min_sup:
                            l_new.pop(new_key)  # 删除支持度小于最小支持度的项
                            discard_count += 1
                    count += 1
                    if count % 1000 == 0 and verbose:
                        print(count)
                        print(time.time() - start_time)
                if not len(l_new) == 0:
                    l_count += 1
                    l.append(l_new)
                    for key in l_new.keys():
                        size_l += sys.getsizeof(l_new[key])
                    if verbose:
                        print(
                            "生成L{0}倒排表成功，满足最小支持度共 {1} 项，丢弃了 {2} 项".format(l_count + 1, len(l[l_count]), discard_count))
                        if len(l[l_count]) > 30000:
                            print("计算{0}项共需要时间为".format(len(l[l_count]), round(time.time() - start_time), 2))
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