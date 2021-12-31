# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         sparktask_overtime_kill
# Description:  spark任务执行超过1个小时自动杀死
# Author:       orange
# Date:         2021/12/4
# -------------------------------------------------------------------------------

from lxml import etree
import os
import re
import io


if __name__ == '__main__':
    """
    description: spark任务执行超过1个小时自动杀死
    author: lj.michale
    """
    print("#######################################")
    os.system('curl http://cdh001:8088/cluster/apps/RUNNING >yarn.html')

    for line in open('yarn.html', 'r').readlines():
        if "spark" in line:
            # 提取spark用户任务的spark ui链接，该服务为长服务，只有1个appid，该服务在spark中可以运行多个sparkjob
            print(line)
            sparkUrl = re.findall('http://cdh001:8088/proxy/application_?\d+_?\d+', line)[0]
    print(sparkUrl)
