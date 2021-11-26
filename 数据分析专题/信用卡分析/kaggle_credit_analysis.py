# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         kaggle_credit_analysis
# Description:
# Author:       orange
# Date:         2021/1/31
# -------------------------------------------------------------------------------
from tkinter import Y

import pandas as pd
from scipy import stats
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import numpy as np

dataset_path = "F:\\datasets\\cs-training.csv"

# 载入数据
data = pd.read_csv(dataset_path)

# 数据集确实和分布情况
data.describe().to_csv('DataDescribe.csv')


# 缺失值处理
# 用随机森林对缺失值预测填充函数
def set_missing(df):
    # 把已有的数值型特征取出来
    process_df = df.ix[:,[5,0,1,2,3,4,6,7,8,9]]
    # 分成已知该特征和未知该特征两部分
    known = process_df[process_df.MonthlyIncome.notnull()].as_matrix()
    unknown = process_df[process_df.MonthlyIncome.isnull()].as_matrix()
    # X为特征属性值
    X = known[:, 1:]
    # y为结果标签值
    y = known[:, 0]
    # fit到RandomForestRegressor之中
    rfr = RandomForestRegressor(random_state=0, n_estimators=200, max_depth=3, n_jobs=-1)
    rfr.fit(X, y)
    # 用得到的模型进行未知特征值预测
    predicted = rfr.predict(unknown[:, 1:]).round(0)
    print(predicted)
    # 用得到的预测结果填补原缺失数据
    df.loc[(df.MonthlyIncome.isnull()), 'MonthlyIncome'] = predicted
    return df


data = set_missing(data)        # 用随机森林填补比较多的缺失值
data = data.dropna()            # 删除比较少的缺失值
data = data.drop_duplicates()   # 删除重复项
data.to_csv('MissingData.csv', index=False)



# 异常值处理
# 年龄等于0的异常值进行剔除
data = data[data['age'] > 0]

# 剔除异常值
data = data[data['NumberOfTime30-59DaysPastDueNotWorse'] < 90]
# 变量SeriousDlqin2yrs取反
data['SeriousDlqin2yrs'] = 1 - data['SeriousDlqin2yrs']

# 数据切分
# 为了验证模型的拟合效果，我们需要对数据集进行切分，分成训练集和测试集。
Y = data['SeriousDlqin2yrs']
X = data.ix[:, 1:]
# 测试集占比30%
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.3, random_state=0)
# print(Y_train)
train = pd.concat([Y_train, X_train], axis=1)
test = pd.concat([Y_test, X_test], axis=1)
clasTest = test.groupby('SeriousDlqin2yrs')['SeriousDlqin2yrs'].count()
train.to_csv('TrainData.csv', index=False)
test.to_csv('TestData.csv', index=False)

# 探索性分析


# 分箱处理
# 定义自动分箱函数
def mono_bin(Y, X, n=20):
    r = 0
    good = Y.sum()
    bad = Y.count() - good
    while np.abs(r) < 1:
        d1 = pd.DataFrame({"X": X, "Y": Y, "Bucket": pd.qcut(X, n)})
        d2 = d1.groupby('Bucket', as_index = True)
        r, p = stats.spearmanr(d2.mean().X, d2.mean().Y)
        n = n - 1
    d3 = pd.DataFrame(d2.X.min(), columns = ['min'])
    d3['min'] = d2.min().X
    d3['max'] = d2.max().X
    d3['sum'] = d2.sum().Y
    d3['total'] = d2.count().Y
    d3['rate'] = d2.mean().Y
    d3['woe'] = np.log((d3['rate']/(1-d3['rate']))/(good/bad))
    d4 = (d3.sort_index(by='min')).reset_index(drop=True)
    print("=" * 60)
    print(d4)
    return d4

# 连续变量离散化

