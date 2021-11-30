# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         TSVM
# Description:
# Author:       orange
# Date:         2021/11/30
# -------------------------------------------------------------------------------

import random
import numpy as np
import sklearn.svm as svm
from sklearn.datasets import make_classification


class TSVM(object):
    '''
    半监督SVM
    '''

    def __init__(self, kernel='linear'):
        self.Cl, self.Cu = 1.5, 0.001
        self.kernel = kernel
        self.clf = svm.SVC(C=1.5, kernel=self.kernel)

    def train(self, X1, Y1, X2):
        N = len(X1) + len(X2)
        # 样本权值初始化
        sample_weight = np.ones(N)
        sample_weight[len(X1):] = self.Cu

        # 用已标注部分训练出一个初始SVM
        self.clf.fit(X1, Y1)

        # 对未标记样本进行标记
        Y2 = self.clf.predict(X2)
        Y2 = Y2.reshape(-1, 1)

        X = np.vstack([X1, X2])
        Y = np.vstack([Y1, Y2])

        # 未标记样本的序号
        Y2_id = np.arange(len(X2))

        while self.Cu < self.Cl:
            # 重新训练SVM, 之后再寻找易出错样本不断调整
            self.clf.fit(X, Y, sample_weight=sample_weight)
            while True:
                Y2_decision = self.clf.decision_function(X2)  # 参数实例到决策超平面的距离
                Y2 = Y2.reshape(-1)
                epsilon = 1 - Y2 * Y2_decision
                negative_max_id = Y2_id[epsilon == min(epsilon)]
                # print(epsilon[negative_max_id][0])
                if epsilon[negative_max_id][0] > 0:
                    # 寻找很可能错误的未标记样本，改变它的标记成其他标记
                    pool = list(set(np.unique(Y1)) - set(Y2[negative_max_id]))
                    Y2[negative_max_id] = random.choice(pool)
                    Y2 = Y2.reshape(-1, 1)
                    Y = np.vstack([Y1, Y2])

                    self.clf.fit(X, Y, sample_weight=sample_weight)
                else:
                    break
            self.Cu = min(2 * self.Cu, self.Cl)
            sample_weight[len(X1):] = self.Cu

    def score(self, X, Y):
        return self.clf.score(X, Y)

    def predict(self, X):
        return self.clf.predict(X)


if __name__ == '__main__':
    features, labels = make_classification(n_samples=200, n_features=3,
                                           n_redundant=1, n_repeated=0,
                                           n_informative=2, n_clusters_per_class=2)
    n_given = 70
    # 取前n_given个数字作为标注集
    X1 = np.copy(features)[:n_given]
    X2 = np.copy(features)[n_given:]
    Y1 = np.array(np.copy(labels)[:n_given]).reshape(-1, 1)
    Y2_labeled = np.array(np.copy(labels)[n_given:]).reshape(-1, 1)
    model = TSVM()
    model.train(X1, Y1, X2)
    accuracy = model.score(X2, Y2_labeled)
    print(accuracy)