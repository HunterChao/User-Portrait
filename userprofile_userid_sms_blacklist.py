
#!/usr/bin/env python
# encoding: utf-8

"""
File: userprofile_userid_sms_blacklist.py
Date: 2018

submit command: 
spark-submit --master yarn --deploy-mode client --driver-memory 1g  --executor-memory 4g --executor-cores 2 
--num-executors 100 userprofile_userid_sms_blacklist.py start-date
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
    start_date_str_2 = strftime(strptime(start_date_str, format_1), format_2)
    old_date_partition_1 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(1), format_1)
    week_day_ago_1 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(14), format_1)
    week_day_ago_2 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(14), format_2)

    target_table = 'dw.userprofile_tag_userid'


    # 累计购买次数
    insertsql= " insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='userid_sms_black') \
                          select 'B120U037_003' as tagid,                     \
                                 user_id as userid,                           \
                                 count(distinct no_idcard) as tagweight,      \
                                 '' as reserve,                               \
                                 '' as reserve1                               \
                            from ods.t_order_auth                             \
                           where dt = '2019-08-28'                            \
                        group by 'B120U037_003', user_id  "
                
                
    spark = SparkSession.builder.appName("userprofile_userid_sms_blacklist").enableHiveSupport().getOrCreate()
    spark.sql(insertsql)
      
if __name__ == '__main__':
    main()





