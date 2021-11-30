# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         LPA
# Description:
# Author:       orange
# Date:         2021/11/30
# -------------------------------------------------------------------------------
import random
import networkx as nx
import matplotlib.pyplot as plt


class LPA():
    '''
    标签传播算法：传播标签来划分社区
    算法终止条件：迭代次数超过设定值
    self.G：图
    return： None
    '''

    def __init__(self, G, iters=10):
        self.iters = iters
        self.G = G

    def train(self):
        max_iter_num = 0  # 迭代次数

        while max_iter_num < self.iters:
            max_iter_num += 1
            print('迭代次数', max_iter_num)

            for node in self.G:
                count = {}  # 记录邻居节点及其标签
                for nbr in self.G.neighbors(node):  # node的邻居节点
                    label = self.G.node[nbr]['labels']
                    count[label] = count.setdefault(label, 0) + 1

                # 找到出现次数最多的标签
                count_items = sorted(count.items(), key=lambda x: -x[-1])
                best_labels = [k for k, v in count_items if v == count_items[0][1]]
                # 当多个标签频次相同时随机选取一个标签
                label = random.sample(best_labels, 1)[0]
                self.G.node[node]['labels'] = label  # 更新标签

    def draw_picture(self):
        # 画图
        node_color = [float(self.G.node[v]['labels']) for v in self.G]
        pos = nx.spring_layout(self.G)  # 节点的布局为spring型
        plt.figure(figsize=(8, 6))  # 图片大小
        nx.draw_networkx(self.G, pos=pos, node_color=node_color)
        plt.show()


if __name__ == "__main__":
    G = nx.karate_club_graph()  # 空手道
    # 给节点添加标签
    for node in G:
        G.add_node(node, labels=node)  # 用labels的状态
    model = LPA(G)
    # 原始节点标签
    model.draw_picture()
    model.train()
    com = set([G.node[node]['labels'] for node in G])
    print('社区数量', len(com))
    # LPA节点标签
    model.draw_picture()