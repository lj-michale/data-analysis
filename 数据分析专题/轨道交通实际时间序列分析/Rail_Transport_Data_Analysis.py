# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         Rail_Transport_Data_Analysis
# Description:  轨道交通数据时间序列分析
# Author:       orange
# Date:         2021/11/26
# -------------------------------------------------------------------------------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from pandas import Series
from math import sqrt
from sklearn.metrics import mean_squared_error
from statsmodels.tsa.seasonal import seasonal_decompose
import statsmodels
import statsmodels.api as sm
from statsmodels.tsa.arima_model import ARIMA


# #############################  数据集准备  #############################
# 去直接读取用pandas读取csv文本文件，并拷贝一份以备用。
train = pd.read_csv("file:///F:\\datasets\\jetrail\\jetrail_train.csv")
test = pd.read_csv("file:///F:\\datasets\\jetrail\\jetrail_test.csv")

train_org = train.copy()
test_org = test.copy()

# 查看数据的列名
print(train.columns)
print(test.columns)

# 查看数据类型
print(train.dtypes)
print(test.dtypes)

# 查看数据大小
print(train.shape)
print(test.shape)

# 查看前200条数据
print(train.head(200))

# 解析日期格式
train['Datetime'] = pd.to_datetime(train.Datetime, format='%d-%m-%Y %H:%M')
test['Datetime'] = pd.to_datetime(test.Datetime, format='%d-%m-%Y %H:%M')
test_org['Datetime'] = pd.to_datetime(test_org.Datetime, format='%d-%m-%Y %H:%M')
train_org['Datetime'] = pd.to_datetime(train_org.Datetime, format='%d-%m-%Y %H:%M')
print(train['Datetime'])
print(test['Datetime'])
print(test_org['Datetime'])
print(train_org['Datetime'])

# 查看时间日期格式解析结果。
print(train.dtypes)
print(train.head())

# 时间序列数据特征工程
for i in (test, train, test_org, train_org):
    i['Year'] = i.Datetime.dt.year
    i['Month'] = i.Datetime.dt.month
    i['day'] = i.Datetime.dt.day
    i['Hour'] = i.Datetime.dt.hour
    # i["day of the week"] = i.Datetime.dt.dayofweek
print(test.head(10))

# 时间戳衍生中，另一常用的方法为布尔特征，即：
# 是否年初/年末
# 是否月初/月末
# 是否周末
# 是否节假日
# 是否特殊日期
# 是否早上/中午/晚上
# 等等

# 下面判断是否是周末，进行特征衍生的布尔特征转换。首先提取出日期时间的星期几。









































