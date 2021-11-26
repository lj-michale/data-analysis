# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         steam_consumption_predict_2
# Description:
# Author:       orange
# Date:         2021/7/17
# -------------------------------------------------------------------------------

# 警告不输出
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression,Lasso,Ridge,RidgeCV,ElasticNet
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import GradientBoostingRegressor,RandomForestRegressor,AdaBoostRegressor,ExtraTreesRegressor
# from xgboost import XGBRegressor
# from lightgbm import LGBMRegressor

# 支持向量机
from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler, PolynomialFeatures

# ###########################  加载数据及特征探索
train = pd.read_csv('E:\\OpenSource\\GitHub\\data-analysis\\数据分析专题\\工业蒸汽量预测\\zhengqi_test.txt', sep='\t')
test = pd.read_csv('E:\\OpenSource\\GitHub\\data-analysis\\数据分析专题\\工业蒸汽量预测\\zhengqi_test.txt', sep='\t')

train['origin'] = 'train'
test['origin'] = 'test'

# 数据聚合
data_all = pd.concat([train, test])

print(data_all.shape)
data_all.head()

# ############################  特征探索
# 总共有28个特征，将不重要的特征就行筛选、删除
# 查看特征分布情况，将训练和测试中分布不均匀的同一特征进行筛选。、删除
plt.figure(figsize=(9, 38 * 6))

for i, col in enumerate(data_all.columns[:-2]):
    cond_train = data_all['origin'] == 'train'
    train_col = data_all[col][cond_train]     # 训练数据

    cond_test = data_all['origin'] == 'test'
    test_col = data_all[col][cond_test]       # 测试数据

    axes = plt.subplot(38, 1, i + 1)
    ax = sns.kdeplot(train_col, shade=True, ax=axes)
    sns.kdeplot(test_col, shade=True, ax=ax)
    plt.legend(['train', 'test'])
    plt.xlabel(col)


# 绘制分布图
plt.figure(figsize=(9, 38 * 6))
for col in data_all.columns[:-2]:
    g = sns.FacetGrid(data_all, col='origin')
    g.map(sns.distplot, col)
plt.show()

# 观察所画图形，筛选出要删除的特征
drop_labels = ["V5", "V9", "V11", "V17", "V22", "V28"]
data_all.drop(drop_labels, axis=1, inplace=True)
plt.show()

# 相关系数
# 相关性系数corr
corr = data_all.corr()
print(corr)
# 通过相关性系数，找到7个相关性不大的特征
cond = corr.loc['target'].abs() < 0.15
drop_labels = corr.loc['target'].index[cond]
drop_labels

# 查看了属性的分布，将分布不好的进行删除
drop_labels = ['V14', 'V21', 'V19', 'V35']
data_all.drop(drop_labels, axis=1, inplace=True)

# 找出相关程度
plt.figure(figsize=(20, 16))  # 指定绘图对象宽度和高度
mcorr = train.corr()  # 相关系数矩阵，即给出了任意两个变量之间的相关系数
mask = np.zeros_like(mcorr, dtype=np.bool)  # 构造与mcorr同维数矩阵 为bool型
mask[np.triu_indices_from(mask)] = True  # 右对角线上部分设置为True
cmap = sns.diverging_palette(220, 10, as_cmap=True)  # 设置颜色
g = sns.heatmap(mcorr, mask=mask, cmap=cmap, square=True, annot=True, fmt='0.2f')  # 热力图（看两两相似度）
plt.show()











