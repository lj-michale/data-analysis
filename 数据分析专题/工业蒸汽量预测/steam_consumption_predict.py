# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         steam_consumption_predict
# Description:   工业蒸汽量预测, https://tianchi.aliyun.com/competition/entrance/231693/information
# Author:       orange
# Date:         2021/7/17
# -------------------------------------------------------------------------------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import matplotlib as mpl
import warnings

import asyncio
import sys
if sys.platform == 'win32' and sys.version_info > (3, 8, 0, 'alpha', 3) :
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

warnings.filterwarnings("ignore")
# 解决中文显示问题
plt.rcParams['font.sans-serif'] = ['SimHei']   # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False   # 用来正常显示负号

train_data_file = "E:\\OpenSource\\GitHub\\data-analysis\\数据分析专题\\工业蒸汽量预测\\zhengqi_test.txt"
test_data_file = "E:\\OpenSource\\GitHub\\data-analysis\\数据分析专题\\工业蒸汽量预测\\zhengqi_test.txt"

train_data = pd.read_csv(train_data_file, sep='\t', encoding='utf-8')
test_data = pd.read_csv(test_data_file, sep='\t', encoding='utf-8')

# #################################  数据探索  #####################################
print(train_data.info())
print(test_data.info())
print(train_data.describe())

plt.style.use('seaborn')
mpl.rcParams['font.family'] = 'serif'

fig = plt.figure(figsize=(4, 6))
sns.barplot(train_data['V0'], orient="v")

column = train_data.columns.to_list()[:39]  # 表头
fig = plt.figure(figsize=(80, 60), dpi=75)
for i in range(38):
    plt.subplot(7, 8, i+1)  # 13行3列子图
    sns.boxplot(train_data[column[i]], orient="v", width=0.5)  # 箱式图
    plt.ylabel(column[i], fontsize=36)


def find_outliers(model, X, y, sigma=3):
    try:
        y_pred = pd.Series(model.predict(X), index=y.index)
    except:
        model.fit(X, y)
        y_pred = pd.Series(model.predict(X), index=y.index)

    resid = y - y_pred
    mean_resid = resid.mean()
    std_resid = resid.std()

    z = (resid - mean_resid) / std_resid
    outliers = z[abs(z) > sigma].index

    print("R2=", model.score(X, y))
    print("mse=", mean_)



# #################################  数据可视化分布  #####################################




