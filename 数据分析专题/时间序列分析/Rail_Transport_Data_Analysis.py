# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         Rail_Transport_Data_Analysis
# Description:  轨道交通数据时间序列分析（以JetRail高铁的乘客数量为例）
# Author:       orange
# Date:         2021/11/26
# 参考资料:
# 时间序列预测的8种常用方法简介
# https://blog.csdn.net/WHYbeHERE/article/details/109732168
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

# #############################  时间序列数据特征工程
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
train['day of the week'] = train.Datetime.dt.dayofweek
# 返回给定日期时间的星期几
print(train.head())


def applyer(row):
    # 再判断day of the week是否是周末（星期六和星期日），是则返回1 ，否则返回0
    if row.dayofweek == 5 or row.dayofweek == 6:
        return 1
    else:
        return 0


temp = train['Datetime']
temp2 = train.Datetime.apply(applyer)
train['weekend'] = temp2
train.index = train['Datetime']

# 对年月乘客总数统计后可视化，看看总体变化趋势。
df = train.drop('ID', 1)
ts = df['Count']
plt.close()
plt.plot(ts, label='Passenger count')
plt.show()

# #############################  探索性数据分析
"""
年
对年进行聚合，求所有数据中按年计算的每日平均客流量，从图中可以看出，随着时间的增长，每日平均客流量增长迅速。
"""
plt.close()
train.groupby('Year')['Count'].mean().plot.bar()
plt.show()

"""
月
对月份进行聚合，求所有数据中按月计算的每日平均客流量，从图中可以看出，春夏季客流量每月攀升，而秋冬季客流量骤减。
"""
plt.close()
train.groupby('Month')['Count'].mean().plot.bar()
plt.show()
"""
年月
对年月份进行聚合，求所有数据中按年月计算的每日平均客流量，从图可知道，几本是按照平滑指数上升的趋势。
"""
temp = train.groupby(['Year', 'Month'])['Count'].mean()
plt.close()
# 乘客人数(每月)
temp.plot()
plt.show()
"""
日
对日进行聚合，求所有数据中每月中的每日平均客流量。从图中可大致看出，在5、11、24分别出现三个峰值，该峰值代表了上中旬的高峰期。
"""
plt.close()
train.groupby('day')['Count'].mean().plot.bar(figsize=(15, 5))
plt.show()

"""
小时
对小时进行聚合，求所有数据中一天内按小时计算的平均客流量，得到了在中(12)晚(19)分别出现两个峰值，该峰值代表了每日的高峰期。
"""
plt.close()
train.groupby('Hour')['Count'].mean().plot.bar()
plt.show()

"""
是否周末
对是否是周末进行聚合，求所有数据中按是否周末计算的平均客流量，发现工作日比周末客流量客流量多近一倍，果然大家都是周末都喜欢宅在家里。
"""
plt.close()
train.groupby('weekend')['Count'].mean().plot.bar()
plt.show()

"""
周
对星期进行聚合统计，求所有数据中按是周计算的平均客流量。
"""
plt.close()
train.groupby('day of the week')['Count'].mean().plot.bar()
plt.show()

"""
时间重采样
◎ 重采样(resampling)指的是将时间序列从一个频率转换到另一个频率的处理过程；
◎ 将高频率数据聚合到低频率称为降采样(downsampling)；
◎ 将低频率数据转换到高频率则称为升采样(unsampling)；
"""
print(train.head())

"""
接下来对训练数据进行对月、周、日及小时多重采样。其实我们分月份进行采样，然后求月内的均值。
事实上重采样，就相当于groupby，只不过是根据月份这个period进行分组。
"""
train = train.drop('ID', 1)
train.timestamp = pd.to_datetime(train.Datetime, format='%d-%m-%Y %H:%M')
train.index = train.timestamp

# 重采样后对其进行可视化，直观地看看其变化趋势。
# 每小时的时间序列
hourly = train.resample('H').mean()
# 换算成日平均值
daily = train.resample('D').mean()
# 换算成周平均值
weekly = train.resample('W').mean()
# 换算成月平均值
monthly = train.resample('M').mean()

# 对测试数据也进行相同的时间重采样处理。

# #############################  划分训练集和验证集
# 到目前为止，我们有训练集和测试集，实际上，我们还需要一个验证集，用来实时验证和调整训练模型。下面直接用索引切片的方式做处理。
Train = train.loc['2012-08-25':'2014-06-24']
valid = train['2014-06-25':'2014-09-25']


























