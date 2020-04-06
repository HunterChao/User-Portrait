#!/usr/bin/env python
# encoding: utf-8

"""
File: userprofile_userid_paidinfo.py
Date: 2018/10/01
submit command: 
submit command: 
spark-submit --master yarn --deploy-mode client --driver-memory 1g  --executor-memory 2g --executor-cores 2 --num-executors 20 userprofile_userid_paidinfo.py start-date

A220U083_001   累计购买金额 
"""

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import sys
import datetime

def main():
    start_date = sys.argv[1]
    start_date_str = str(start_date)
    format_1 = "%Y%m%d"
    format_2 = "%Y-%m-%d"
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    old_date_partition = strftime(strptime(start_date_str, format_1), format_2)
    target_table = 'dw.userprofile_tag_userid' 
    
    # 累计购买次数
    regist_notpaid = " insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='userid_paidinfo') \
                          select 'A220U083_001' as tagid,                     \
                                 user_id as userid,                           \
                                 count(*) as tagweight,                       \
                                 '' as reserve,                               \
                                 '' as reserve1                               \
                            from ods.t_order_auth                            \
                           where dt = '2019-08-28'                            \
                        group by 'A220U083_001', user_id  "
    
    
    spark = SparkSession.builder.appName("userid_paidinfo").enableHiveSupport().getOrCreate()
    spark.sql(regist_notpaid)
      
if __name__ == '__main__':
    main()

