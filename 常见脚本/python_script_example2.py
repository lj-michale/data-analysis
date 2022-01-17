# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         python_script_example2
# Description:
# Author:       orange
# Date:         2022/1/17
# -------------------------------------------------------------------------------

import argparse
import warnings

warnings.filterwarnings('ignore')


if __name__ == '__main__':
    # 创建一个解析对象
    parser = argparse.ArgumentParser(description='Process some integers.')
    # 向该对象中添加需要关注的参数与选项
    parser.add_argument('--param001',
                        type=str,
                        default='../data/used_car_train_20200313.csv',
                        help='python传入参数param1')
    parser.add_argument('--param002',
                        type=str,
                        default='../data/used_product_train_20200313.csv',
                        help='python传入参数param2')
    parser.add_argument('--param003',
                        type=str,
                        default='../data/used_car_train_20200313.csv',
                        help='python传入参数param3')
    parser.add_argument('--param004',
                        type=str,
                        default='../data/used_product4_train_20200313.csv',
                        help='python传入参数param4')
    parser.add_argument('--param005',
                        type=str,
                        default='../data/used_ca5_train_20200313.csv',
                        help='python传入参数param5')
    parser.add_argument('--param006',
                        type=str,
                        default='../data/used_product6_train_20200313.csv',
                        help='python传入参数param6')
    # 传入参数解析
    args = parser.parse_args()
    print(args)
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--train_data',
    #                     type=str,
    #                     default='../data/used_car_train_20200313.csv',
    #                     help='path to original train data'
    #                     )

    # parser = argparse.ArgumentParser(description='Process some integers.')
    # parser.add_argument('integers',
    #                     metavar='N',
    #                     type=int,
    #                     nargs='+',
    #                     help='an integer for the accumulator')
    # parser.add_argument('--sum',
    #                     dest='accumulate',
    #                     action='store_const',
    #                     const=sum,
    #                     default=max,
    #                     help='sum the integers (default: find the max)')
    # args = parser.parse_args()
    # print(args.accumulate(args.integers))
