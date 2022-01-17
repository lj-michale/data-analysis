# -*- coding: utf-8 -*-
# @Time    : 2021/01/02 上午8:50
# @Author  : LJ.Michale
# @Nickname : 黑白时差
# @FileName: blo_id_mapping.py
# @Software: PyCharm
# @PythonVersion: python3.7


import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 设置环境变量
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_171.jdk/Contents/Home'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'

spark = SparkSession.builder.master("local").appName("id_mapping").getOrCreate()


# 1.数据预处理：将数据分为需要id_mapping 和 不需要id_mapping 两部分
# 2.特征工程处理：将需要做id_mapping 的数据处理成user，item，score 形式
# 3.利用杰卡德对需要做id_mapping 的用户进行合并
# 4.构建用户中间过程表，用户根据中间过程表进行合并
# 5.增量数据进行用户合并

# 1 数据预处理：删选出需要做idmapping的用户（用户各个业务系统中的id出现了多次则需要做idmapping，如果全部元素都只出现了一次则不做idmapping）

def mapfromarrays(keys, values):
    # 高版本中有方法map_from_arrays可以直接用,利用函数的时候，传进去的数据类型一定要是能够识别的数据类型，公司集群老版本pyspark，没有这个方法
    # b = df_idmapping_need.select(F.map_from_arrays(F.array(F.lit('te'), F.lit('me')), F.array(F.col('tel'), F.col("member_id"))))
    # df_idmapping_need = spark.read.csv("./df_idmapping_need.csv", header=True, inferSchema=True)
    return dict(zip(keys, values))


def id_mapping_filter():
    # 1 数据预处理
    # 1.1. 每行数据增加其各个id在全部数据中出现的次数
    # 利用本地文件作为数据源进行id_mapping，方便大家直接下载跑起来
    df_train = spark.read.csv("./train_sample.csv", header=True, inferSchema=True)
    # 去重， 把完全重复的取出（数据拿过来的时候已经进行了去重，可以省略）
    df_train = df_train.drop_duplicates()
    df_train.createOrReplaceTempView("df_train_view")
    # 统计出每个user中各个id出现的次数，然后把次数加到每行记录
    sql_freq = """
    select *,
    count(tel) over(partition by tel) as freq_tel,
    count(wx_common_id) over(partition by wx_common_id) as  freq_common,
    count(wx_open_id) over(partition by wx_open_id) as freq_open,
    count(alipay_id) over(partition by alipay_id) freq_alipay,
    count(member_id) over(partition by member_id) as freq_member,
    count(o2o_id) over(partition by o2o_id) as freq_o2o
    from df_train_view  t
    """
    spark.sql(sql_freq).createOrReplaceTempView("df_idmapping_base_view")
    # 1.2根据各id出现的频次freq，筛选哪些用户需要mapping
    # 1.2.1：找出不需要做id_mapping的用户， 所有id均未重复出现，为唯一id用户，不需要做相似度合并
    sql_no_mapping = """
    select one_id,weight,tel,wx_common_id,wx_open_id,alipay_id,member_id,customer_id,o2o_id,create_time,update_time
    from df_idmapping_base_view
    where (freq_tel<2 or freq_tel is null ) and (freq_common<2 or freq_common is null)
    and (freq_open<2 or freq_open is null) and (freq_alipay<2 or freq_alipay is null)
    and (freq_member<2 or freq_member is null) and (freq_o2o<2 or freq_o2o is null)
    """
    df_no_idmapping = spark.sql(sql_no_mapping).createOrReplaceTempView("df_no_mapping_view")
    # 1.2.2 找出需要做id_mapping的用户：重复出现的id用户，需要做相似度合并
    sql_idmapping_need = """
    select one_id,weight,tel,wx_common_id,wx_open_id,alipay_id,member_id,customer_id,o2o_id,create_time,update_time
    from df_idmapping_base_view
    where (freq_tel>=2) or (freq_common>=2) or (freq_open>=2) or (freq_alipay>=2) or (freq_member>=2) or (freq_o2o>=2)
    """
    df_idmapping_need = spark.sql(sql_idmapping_need)
    df_idmapping_need.createOrReplaceTempView("df_idmapping_need_view")
    return df_idmapping_need


def extract_feature(df_idmapping_need):
    # 二 特征工程，这里直接udf把数据转化成user，item，score三列

    map_from_arrays = F.udf(mapfromarrays, MapType(StringType(), StringType()))
    # F.lit 新增一列并命名，F.col取出某列的值 F.array() 相当于python中的list
    df_idmapping_need_exp = df_idmapping_need.select("one_id",
                                                     map_from_arrays(F.array(F.lit('te'), F.lit("me"), F.lit("co"),
                                                                             F.lit("al"), F.lit("o2"), F.lit("op")),
                                                                     F.array(F.col("tel"), F.col("member_id"),
                                                                             F.col("wx_common_id"),
                                                                             F.col("alipay_id"), F.col("o2o_id"),
                                                                             F.col("wx_open_id"))
                                                                     ).alias("map")
                                                     )
    # explode 把数组重新打开,并且一行变多行,先是把多列合成一列，然后再把列拆开，相当于行转列
    df_idmapping_need_exp = df_idmapping_need_exp.select('one_id', F.explode("map"))
    df_idmapping_need_exp = df_idmapping_need_exp.filter(df_idmapping_need_exp.value.isNotNull())
    # print(df_idmapping_need_exp.show())
    # 给每一个元素打上权重
    df_idmapping_need_exp = df_idmapping_need_exp.select(df_idmapping_need_exp.one_id.alias("user"),
                                                         F.concat(df_idmapping_need_exp.key, F.lit("_"),
                                                                  df_idmapping_need_exp.value).alias("item"),
                                                         F.when(df_idmapping_need_exp.key == "te", 25).when(
                                                             df_idmapping_need_exp.key == "me", 15).
                                                         when(df_idmapping_need_exp.key == "co", 15).when(
                                                             df_idmapping_need_exp.key == "al", 20).
                                                         when(df_idmapping_need_exp.key == "o2", 5).when(
                                                             df_idmapping_need_exp.key == "op", 10).otherwise(0)
                                                         .alias("score")
                                                         )
    return df_idmapping_need_exp
    # +----+--------------+-----+
    # | user | item | score |
    # +----+--------------+-----+
    # | a_7 | op_o_1 | 10 |
    # | a_7 | te_18221562928 | 25 |
    # | a_7 | o2_e_6 | 5 |


def jacaard_simi(df_idmapping_need_exp):
    # userbase-cf，相似的用户被归位同一用户；用Jaccard算法计算用户相似度(取最相似的用户合并)
    # 1.计算每个用户有多少个item
    # 2.筛选出有对应用户的item
    # 3.利用jaccard相似度计算用户之间的相似度并选出最相似的用户进行合并
    # 4.根据用户注册时间将最早注册的用户作为合并后的用户
    # 准备数据。相似度算法训练数据：生成每个用户所对应的item及item权重字典表,和item总数
    # collect_list 把所有元素拿过来合成一个list，后面元素如果在生成一个list，是按照顺序一一对应的
    map_from_arrays = F.udf(mapfromarrays, MapType(StringType(), IntegerType()))
    df_idmapping_need_train = df_idmapping_need_exp.groupby('user').agg(
        map_from_arrays(F.collect_list("item"), F.collect_list("score")).alias("itemscore"),
        F.count("item").alias("item_n"))
    # user|           itemscore|item_n|
    # +----+--------------------+------+
    # | a_6|[al_a_4 -> 20, me...|     6|
    # | a_5|[op_o_1 -> 10, co...|     3|
    # 生成每个item对应的user列表和user数
    df_item_users = df_idmapping_need_exp.groupby("item").agg(F.collect_set("user").alias("user"),
                                                              F.count("user").alias("user_n"))
    # item|                user|user_n|
    # +------------------+--------------------+------+
    # |            me_m_7|               [a_8]|     1|
    # |te_1.8221562924E10|          [a_4, a_2]|     2|
    # 去item对应多个user的item，排除item只对应一个user的item
    df_item_users_more = df_item_users.filter(df_item_users.user_n > 1).select("item", "user")
    # 创建虚拟表，根据item关联相似的user
    # df_item_users_more.select("item", F.explode("user").alias("user")).createOrReplaceTempView("df_item_user_view")
    df_item_users_more.select("item", F.explode("user").alias("user")).createOrReplaceTempView("df_item_user_view")
    df_idmapping_need_exp.select("user", "item", "score").createOrReplaceTempView("df_user_item_view")
    df_idmapping_need_train.createOrReplaceTempView("df_idmapping_need_train_view")
    # 算法步骤二：计算用户item交集，统计连个user间有多少共同的item （CUV）
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    sql_common_item = 'select u_i.user, i_u.user as ruser, count(1) as common_n,sum(u_i.score) common_score from df_user_item_view u_i inner join df_item_user_view i_u on u_i.item=i_u.item and u_i.user!=' \
                      'i_u.user group by u_i.user, i_u.user having count(1) > 0'
    df_idmapping_common = spark.sql(sql_common_item)
    # print(df_idmapping_common.show())
    # user | ruser | common_n | common_score |
    # +----+-----+--------+------------+
    # | a_7 | a_1 | 1 | 10 |
    # | a_7 | a_5 | 1 | 10 |
    df_idmapping_common.createOrReplaceTempView("df_idmapping_common_view")
    # 计算jaccard用户相似度t2.v/(t1.item_n+t3.item_n-t2.v)，分子为交集，分母为并集减去交集
    # map_from_arrays = F.udf(mapfromarrays, MapType(StringType(), FloatType()))
    map_from_arrays = F.udf(mapfromarrays, MapType(StringType(), FloatType()))
    # 根据user和相似user的items数和这两个用户相似item的数量计算两者之间的相似度,根据相似度得分倒排序
    sql_cuv = "select t1.user, t1.item_n user_n, t2.ruser, t3.item_n ruser_n, t2.common_n ruser_c,t2.common_score/(t1.item_n+t3.item_n-t2.common_n) ruser_w," \
              "rank() over(partition by t1.user order by t2.common_score/(t1.item_n+t3.item_n-t2.common_n) desc) K from df_idmapping_need_train_view t1 " \
              "left join df_idmapping_common_view t2 on t1.user=t2.user " \
              "left join df_idmapping_need_train_view t3 on t2.ruser=t3.user"
    df_idmapping_cuv = spark.sql(sql_cuv)
    df_idmapping_cuv.createOrReplaceTempView("df_idmapping_cuv_view")
    # print(df_idmapping_cuv.show())
    # user | user_n | ruser | ruser_n | ruser_c | ruser_w | K |
    # +----+------+-----+-------+-------+-------------------+---+
    # | a_6 | 6 | a_5 | 3 | 1 | 1.875 | 1 |
    # | a_6 | 6 | a_1 | 6 | 1 | 1.3636363636363635 | 2
    # 选出和每一个用户最相似的用户作为其应该被合并的对象，然后取相似用户中最早注册的用户作为最终的会员
    sql_jaccard_filter = "select cuv.user, cuv.ruser, need_view.create_time,need_view_1.create_time simi_create_time  from df_idmapping_cuv_view cuv " \
                         " inner join df_idmapping_need_view need_view on cuv.user=need_view.one_id" \
                         " inner join df_idmapping_need_view need_view_1 on cuv.ruser=need_view_1.one_id " \
                         "where k=1"
    merge_oneid_jaccard = spark.sql(sql_jaccard_filter)
    merge_oneid_jaccard.createOrReplaceTempView("merge_oneid_jaccard_view")
    # print(merge_oneid_jaccard.show())
    # user|ruser|         create_time|    simi_create_time|
    # +----+-----+--------------------+--------------------+
    # | a_6|  a_5|2019-07-05 11:44:...|2019-07-05 11:56:...|
    # | a_5|  a_1|2019-07-05 11:56:...|2019-07-05 11:40:...|
    # | a_5|  a_2|2019-07-05 11:56:...|2019-07-05 11:41
    # 完成初步用户合并，选择创建时间小的为最后用户，如果创建时间相同就去one_id 小的
    sql_merge_oneid_initial = "select user, ruser,case when create_time < simi_create_time then user when create_time = simi_create_time then " \
                              "case when user > ruser then ruser else user end else ruser end merge_user, create_time,simi_create_time from merge_oneid_jaccard_view  "
    merge_oneid_initial = spark.sql(sql_merge_oneid_initial)
    merge_oneid_initial.createOrReplaceTempView("merge_oneid_initial_view")
    # print(merge_oneid_initial.show())
    # user|ruser|merge_user|         create_time|    simi_create_time|
    # +----+-----+----------+--------------------+--------------------+
    # | a_6|  a_5|       a_6|2019-07-05 11:44:...|2019-07-05 11:56:...|
    # | a_5|  a_1|       a_1|2019-07-05 11:56:...|2019-07-05 11:40:...|
    # | a_5|  a_2|       a_2|2019-07-05 11:56:...|2019-07-05 11:41:...|


def final_merge_oneid():
    # 1.根据drop_user 进行第一次合并
    # 2.第一次合并结果出现在了第二次合并结果中，还需要进行合并
    # 1.1 筛选出被合并的用户（合并过程中被删除的用户）
    merge_oneid_drop_user = "select case when user != merge_user then user else ruser end as drop_user,user,ruser,merge_user, create_time, simi_create_time from merge_oneid_initial_view"
    spark.sql(merge_oneid_drop_user).createOrReplaceTempView("merge_oneid_drop_user_view")
    # drop_user | user | ruser | merge_user | create_time | simi_create_time |
    # +---------+----+-----+----------+--------------------+--------------------+
    # |      a_5| a_6|  a_5|       a_6|2019-07-05 11:44:...|2019-07-05 11:56:...|
    # |      a_5| a_5|  a_1|       a_1|2019-07-05 11:56:...|2019-07-05 11:40:...|
    # |      a_5| a_5|  a_2|       a_2|2019-07-05 11:56:...|2019-07-05 11:41:...|
    # 1.2 根据被合并的用户再进行合并
    merge_oneid_drop_user_2 = "select * from " \
                              "(select *,row_number() over(partition by drop_user order by " \
                              "case when user=merge_user then create_time else simi_create_time end asc) rank " \
                              "from merge_oneid_drop_user_view) a where rank=1"
    spark.sql(merge_oneid_drop_user_2).createOrReplaceTempView("merge_oneid_drop_user_2_view")
    # 根据drop_user 合并用户
    # drop_user|user|ruser|merge_user|         create_time|    simi_create_time|rank|
    # +---------+----+-----+----------+--------------------+--------------------+----+
    # |      a_5| a_5|  a_1|       a_1|2019-07-05 11:56:...|2019-07-05 11:40:...|   1|
    # |      a_8| a_8|  a_1|       a_1|2019-07-05 11:46:...|2019-07-05 11:40:...|   1|
    # |      a_2| a_1|  a_2|       a_1|2019-07-05 11:40:...|2019-07-05 11:41:...|   1|
    # |      a_9| a_9|  a_1|       a_1|2019-07-05 11:47:...|2019-07-05 11:40:...|   1|
    # |      a_4| a_4|  a_2|       a_2|2019-07-05 11:43:...|2019-07-05 11:41:...|   1|
    # |     a_14|a_13| a_14|      a_13|2019-07-05 11:46:...|2019-07-05 11:46:...|   1|
    # +---------+----+-----+----------+--------------------+--------------------+----+
    # 根据drop_user,将所有用户在进行一次合并
    merge_oneid_drop_user_f = "select oneid_2.drop_user,oneid_2.user, oneid_2.merge_user, oneid_3.merge_user merge_d1_user, oneid_2.create_time, oneid_2.simi_create_time from merge_oneid_drop_user_view oneid_2 " \
                              "inner join merge_oneid_drop_user_2_view oneid_3 " \
                              "on oneid_2.drop_user=oneid_3.drop_user"
    # print(spark.sql(sql_merge_oneid_4).show())
    spark.sql(merge_oneid_drop_user_f).createOrReplaceTempView("merge_oneid_drop_user_f_view")
    # print(spark.sql(merge_oneid_drop_user_f).show())
    # drop_user|user|merge_user|merge_d1_user|         create_time|    simi_create_time|
    # +---------+----+----------+-------------+--------------------+--------------------+
    # |      a_5| a_6|       a_6|          a_1|2019-07-05 11:44:...|2019-07-05 11:56:...|
    # |      a_5| a_5|       a_1|          a_1|2019-07-05 11:56:...|2019-07-05 11:40:...|
    # |      a_5| a_5|       a_2|          a_1|2019-07-05 11:56:...|2019-07-05 11:41:...|
    # |      a_5| a_7|       a_7|          a_1|2019-07-05 11:45:...|2019-07-05 11:56:...|
    # |      a_8| a_8|       a_1|          a_1|2019-07-05 11:46:...|2019-07-05 11:40:...|
    # |      a_8| a_8|       a_2|          a_1|2019-07-05 11:46:...|2019-07-05 11:41:...|
    # |      a_2| a_1|       a_1|          a_1|2019-07-05 11:40:...|2019-07-05 11:41:...|
    # |      a_2| a_2|       a_1|          a_1|2019-07-05 11:41:...|2019-07-05 11:40:...|
    # |      a_9| a_9|       a_1|          a_1|2019-07-05 11:47:...|2019-07-05 11:40:...|
    # 2 第一次结果有在第二次结果出现的全部过滤：过滤掉归为merge_d1_user并且在merge_user 中出现且merge_user!=merge_d1_user
    # 2.1 选出不能作为最终结果的用户
    merge_oneid_middle_user = "select distinct m_4.merge_user, m_4.merge_d1_user from merge_oneid_drop_user_f_view m_4 inner join (select DISTINCT  merge_d1_user from merge_oneid_drop_user_f_view) m_4_2  on m_4.merge_user = m_4_2.merge_d1_user  " \
                              "where m_4.merge_user != m_4.merge_d1_user "
    spark.sql(merge_oneid_middle_user).createOrReplaceTempView("merge_oneid_middle_user_view")
    # print(spark.sql(merge_oneid_middle_user).show())
    # merge_user|merge_d1_user|
    # +----------+-------------+
    # |       a_2|          a_1|
    # +----------+-------------
    # 2.2根据再次被合并的结果，将中间用户再次合并
    merge_oneid_middle_fin = "select a.*, case when b.merge_d1_user is null then a.merge_d1_user  else b.merge_d1_user end merge_fin_user from merge_oneid_drop_user_f_view a left join merge_oneid_middle_user_view b on a.merge_d1_user=b.merge_user "
    spark.sql(merge_oneid_middle_fin).createOrReplaceTempView('merge_oneid_middle_fin_view')
    # 用户合并完成，加上 不需要进行id_mapping 的用户就是全部的用户
    # +---------+----+----------+-------------+--------------------+--------------------+--------------+
    # |drop_user|user|merge_user|merge_d1_user|         create_time|    simi_create_time|merge_fin_user|
    # +---------+----+----------+-------------+--------------------+--------------------+--------------+
    # |      a_5| a_6|       a_6|          a_1|2019-07-05 11:44:...|2019-07-05 11:56:...|           a_1|
    # |      a_5| a_5|       a_1|          a_1|2019-07-05 11:56:...|2019-07-05 11:40:...|           a_1|
    # |      a_5| a_5|       a_2|          a_1|2019-07-05 11:56:...|2019-07-05 11:41:...|           a_1|
    # |      a_5| a_7|       a_7|          a_1|2019-07-05 11:45:...|2019-07-05 11:56:...|           a_1|
    # |      a_8| a_8|       a_1|          a_1|2019-07-05 11:46:...|2019-07-05 11:40:...|           a_1|
    # |      a_8| a_8|       a_2|          a_1|2019-07-05 11:46:...|2019-07-05 11:41:...|           a_1|
    # |      a_2| a_1|       a_1|          a_1|2019-07-05 11:40:...|2019-07-05 11:41:...|           a_1|
    # |      a_2| a_2|       a_1|          a_1|2019-07-05 11:41:...|2019-07-05 11:40:...|           a_1|
    # |      a_9| a_9|       a_1|          a_1|2019-07-05 11:47:...|2019-07-05 11:40:...|           a_1|
    # |      a_9| a_9|       a_2|          a_1|2019-07-05 11:47:...|2019-07-05 11:41:...|           a_1|
    # |     a_14|a_13|      a_13|         a_13|2019-07-05 11:46:...|2019-07-05 11:46:...|          a_13|
    # |     a_14|a_14|      a_13|         a_13|2019-07-05 11:46:...|2019-07-05 11:46:...|          a_13|
    # |      a_4| a_4|       a_2|          a_2|2019-07-05 11:43:...|2019-07-05 11:41:...|           a_1|
    # |      a_4| a_3|       a_3|          a_2|2019-07-05 11:42:...|2019-07-05 11:43:...|           a_1|
    # +---------+----+----------+-------------+--------------------+--------------------+--------------+


def item_user_create():
    # 构建item_user 中间关系表，增量数据直接可根据中间关系表关联，增量数据关联到的，直接合并mid，关联不到的再次进行id_mapping
    # 最终将需要合并的用户合并，同时加上最终合并用户的创建时间
    # 1.需要合并的用户进行元素匹配
    sql_merge_item = "select need_view.*, fin_du.merge_fin_user,need_view_1.create_time merge_user_create_time from df_idmapping_need_view need_view inner join " \
                     "(select DISTINCT user, merge_fin_user from merge_oneid_middle_fin_view) fin_du " \
                     "on need_view.one_id = fin_du.user " \
                     "inner join df_idmapping_need_view need_view_1 on need_view_1.one_id=fin_du.merge_fin_user"
    spark.sql(sql_merge_item).createOrReplaceTempView("merge_user_fin_view")
    # one_id|weight|        tel|wx_common_id|wx_open_id|alipay_id|member_id|customer_id|o2o_id|         create_time|update_time|merge_fin_user|merge_user_create_time|
    # +------+------+-----------+------------+----------+---------+---------+-----------+------+--------------------+-----------+--------------+----------------------+
    # |   a_7|    95|18221562928|         u_6|       o_1|      a_6|      m_6|        c_6|   e_6|2019-07-05 11:45:...|       null|           a_1|  2019-07-05 11:40:...|
    # |  a_13|    95|18221562934|        u_12|       o_7|     a_12|     m_12|       c_12|  e_12|2019-07-05 11:46:...|       null|          a_13|  2019-07-05 11:46:...|
    # |   a_1|    95|18221562923|         u_1|       o_1|      a_1|      m_1|        c_1|   e_1|2019-07-05 11:40:...|       null|           a_1|  2019-07-05 11:40:...|
    # |   a_9|    95|18221562930|         u_8|       o_8|      a_8|      m_1|        c_8|   e_8|2019-07-05 11:47:...|       null|           a_1|  2019-07-05 11:40:...|
    # |   a_4|    95|18221562924|         u_3|       o_3|      a_3|      m_2|        c_3|   e_1|2019-07-05 11:43:...|       null|           a_1|  2019-07-05 11:40:...|
    # |   a_5|    35|       null|         u_1|       o_1|     null|     null|        c_1|   e_1|2019-07-05 11:56:...|       null|           a_1|  2019-07-05 11:40:...|
    # 2.构建中间过程表，需要id_mapping 用户和不需要id_mapping 用户都需要匹配item到user
    # 2.1 整合tel元素到最终用户，一个tel只能对应一个用户，一个用户可以对应多个tel
    merge_tel_item = "select * from (select tel, merge_fin_user merge_user, merge_user_create_time, row_number() over(partition by tel order by merge_user_create_time asc, merge_fin_user asc) rank from merge_user_fin_view) where rank=1"
    merge_tel_item = "select * from (select tel,merge_user, merge_user_create_time  from (select tel, merge_fin_user merge_user, merge_user_create_time, row_number() over(partition by wx_common_id order by merge_user_create_time asc, merge_fin_user asc) rank from merge_user_fin_view where wx_common_id is not null ) where rank=1 " \
                     "union all select tel,one_id merge_user, create_time merge_user_create_time from df_no_mapping_view where wx_common_id is not null) a"
    spark.sql(merge_tel_item).createOrReplaceTempView("merge_tel_item_view")
    # 2.2. 整合wx_common_id元素到最终用户，一个wx_common_id只能对应一个用户，一个用户可以对应多个tel
    merge_wx_common_item = "select * from (select wx_common_id,merge_user, merge_user_create_time  from (select wx_common_id, merge_fin_user merge_user, merge_user_create_time, row_number() over(partition by wx_common_id order by merge_user_create_time asc, merge_fin_user asc) rank from merge_user_fin_view where wx_common_id is not null ) where rank=1 " \
                           "union all select wx_common_id,one_id merge_user, create_time merge_user_create_time from df_no_mapping_view where wx_common_id is not null) a"
    spark.sql(merge_wx_common_item).createOrReplaceTempView("merge_wx_common_item_view")
    # wx_common_id|merge_user|merge_user_create_time|
    # +------------+----------+----------------------+
    # |         u_2|       a_1|  2019-07-05 11:40:...|
    # |         u_6|       a_1|  2019-07-05 11:40:...|
    # |         u_7|       a_1|  2019-07-05 11:40:...|
    # |        u_12|      a_13|  2019-07-05 11:46:...|
    # 2.3 整合wx_open_id元素到最终用户，一个wx_open_id只能对应一个用户，一个用户可以对应多个tel
    merge_wx_open_item = "select * from (select wx_open_id,merge_user, merge_user_create_time  from (select wx_open_id, merge_fin_user merge_user, merge_user_create_time, row_number() over(partition by wx_common_id order by merge_user_create_time asc, merge_fin_user asc) rank from merge_user_fin_view where wx_common_id is not null ) where rank=1 " \
                         "union all select wx_open_id,one_id merge_user, create_time merge_user_create_time from df_no_mapping_view where wx_common_id is not null) a"
    spark.sql(merge_wx_open_item).createOrReplaceTempView(
        "merge_wx_open_item_view")  # # 3.2.4 整合alipay_id元素到最终用户，一个alipay_id只能对应一个用户，一个用户可以对应多个tel
    # 2.4 整合alipay_id元素到最终用户，一个alipay_id只能对应一个用户，一个用户可以对应多个tel

    merge_wx_alipay_item = "select * from (select alipay_id,merge_user, merge_user_create_time  from (select alipay_id, merge_fin_user merge_user, merge_user_create_time, row_number() over(partition by wx_common_id order by merge_user_create_time asc, merge_fin_user asc) rank from merge_user_fin_view where wx_common_id is not null ) where rank=1 " \
                           "union all select alipay_id,one_id merge_user, create_time merge_user_create_time from df_no_mapping_view where wx_common_id is not null) a"
    spark.sql(merge_wx_alipay_item).createOrReplaceTempView("merge_alipay_item_view")

    # 3.2.5 整合member_id元素到最终用户，一个member_id只能对应一个用户，一个用户可以对应多个tel
    merge_member_item = "select * from (select member_id,merge_user, merge_user_create_time  from (select member_id, merge_fin_user merge_user, merge_user_create_time, row_number() over(partition by wx_common_id order by merge_user_create_time asc, merge_fin_user asc) rank from merge_user_fin_view where wx_common_id is not null ) where rank=1 " \
                        "union all select member_id,one_id merge_user, create_time merge_user_create_time from df_no_mapping_view where wx_common_id is not null) a"
    spark.sql(merge_member_item).createOrReplaceTempView("merge_member_item_view")

    # # 3.2.6 整合o2o_id元素到最终用户，一个o2o_id只能对应一个用户，一个用户可以对应多个o2o_id

    merge_o2o_item = "select * from (select o2o_id,merge_user, merge_user_create_time  from (select o2o_id, merge_fin_user merge_user, merge_user_create_time, row_number() over(partition by wx_common_id order by merge_user_create_time asc, merge_fin_user asc) rank from merge_user_fin_view where wx_common_id is not null ) where rank=1 " \
                     "union all select o2o_id,one_id merge_user, create_time merge_user_create_time from df_no_mapping_view where wx_common_id is not null) a"
    spark.sql(merge_o2o_item).createOrReplaceTempView("merge_o2o_item_view")


def incream_merge():
    # 增量用户进行id_mapping
    # TODO
    # 1.增量用户用item直接和中间过程表（item_user_create 创建的表）关联，关联不上的，重复上面的步骤做id_mapping
    # 2. 关联上的，直接根据权重确定最终为哪个用户，然后在做中间过程表
    pass


def schdule():
    # 1.过滤数据，将数据分为需要id_mapping 和不需要id_mapping两部分
    df_idmapping_need = id_mapping_filter()
    # 2.处理数据，把数据处理成item_user 和user_item 形式
    df_idmapping_need_exp = extract_feature(df_idmapping_need)
    # 3.利用杰卡德相似度合并用户
    jacaard_simi(df_idmapping_need_exp)
    # 4.再次合并用户
    final_merge_oneid()
    # 5.穿件item_user 中间过程表，增量数据根据中间过程表id_mapping
    item_user_create()


if __name__ == '__main__':
    schdule()


