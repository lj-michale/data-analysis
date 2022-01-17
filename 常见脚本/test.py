# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         test
# Description:
# Author:       orange
# Date:         2022/1/17
# -------------------------------------------------------------------------------
import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--integers', metavar='N', type=int, nargs='+',
                    help='an integer for the accumulator')
parser.add_argument('--sum', dest='accumulate', action='store_const',
                    const=sum, default=max,
                    help='sum the integers (default: find the max)')

args = parser.parse_args()
print(args.accumulate(args.integers))

parser.add_argument('--param1',
                    metavar='N',
                    type=str,
                    help='an string chart')



