#!/usr/bin/env python
# encoding: utf-8

"""
File: userprofile_userid_return_goods_rate.py
Date: 2018/10/01
submit command: 
spark-submit --master yarn --deploy-mode client --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50 userprofile_userid_return_goods_rate.py start-date
"""
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import sys
import datetime

# 用户近30日内退货率   B220U071_001
def main():
    start_date = sys.argv[1]
    start_date_str = str(start_date)
    format_1 = "%Y%m%d"
 
    target_table = 'dw.profile_tag_user'  
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    old_date_partition_1 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(1), format_1)
    month_day_ago_1 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(30), format_1)
    
    # 用户近30日订单量
    user_paid_30_orders = " select t1.user_id,                                                                                       \
                                   count(distinct t1.order_id) as paid_orders                                                        \
                              from dw.dw_order_fact t1                                                                               \
                             where t1.pay_status in (1,3)                                                                            \
                               and concat(substr(t1.pay_time,1,4),substr(t1.pay_time,6,2),substr(t1.pay_time,9,2)) >= "+"'"+month_day_ago_1+"'"+"   \
                               and concat(substr(t1.pay_time,1,4),substr(t1.pay_time,6,2),substr(t1.pay_time,9,2)) <= "+"'"+start_date_str+"'"+"    \
                          group by t1.user_id "

    # 用户近30日订单退货量
    user_paid_30_return = "  select t1.user_id,                                                                                    \
                                    count(distinct t1.returned_order_id) as returned_orders                                        \
                               from jolly.who_wms_returned_order_info  t1                                                          \
                         inner join dw.dw_order_fact t2                                                                            \
                                 on (t1.user_id = t2.user_id and t1.returned_order_id = t2.order_id)                               \
                              where t1.return_status in (0,1,2,3,4,5,6,7,8)                                                        \
                                and t1.site_id in (0,1)                                                                            \
                                and from_unixtime(t1.returned_time,'yyyyMMdd') >= "+"'"+month_day_ago_1+"'"+"                      \
                                and from_unixtime(t1.returned_time,'yyyyMMdd') <= "+"'"+start_date_str+"'"+"                       \
                                and t2.pay_status in (1,3)                                                                         \
                                and concat(substr(t2.pay_time,1,4),substr(t2.pay_time,6,2),substr(t2.pay_time,9,2)) >= "+"'"+month_day_ago_1+"'"+"  \
                                and concat(substr(t2.pay_time,1,4),substr(t2.pay_time,6,2),substr(t2.pay_time,9,2)) <= "+"'"+start_date_str+"'"+"   \
                           group by t1.user_id "
    
    user_paid_30_rate = "  insert overwrite table " +target_table+" partition(data_date ="+"'"+start_date_str+"'"+",tagtype='return_goods_rate') \
                          select 'B220U071_001' as tagid,                                  \
                                 t.user_id as userid,                                      \
                                 round(t.returned_orders/t.paid_orders, 2) as tagweight,   \
                                 '' as tagranking,                                         \
                                 '' as reserve,                                            \
                                 '' as reserve1                                            \
                            from (                                                         \
                                  select nvl(t2.user_id,t1.user_id) as user_id,            \
                                         nvl(t2.returned_orders, 0) as returned_orders,    \
                                         t1.paid_orders                                    \
                                    from user_paid_30_orders t1                            \
                         full outer join user_paid_30_return t2                            \
                                      on t1.user_id = t2.user_id                           \
                                  ) t                                                      \
                         group by 'B220U071_001',                                          \
                                   t.user_id,                                              \
                                   round(t.returned_orders/t.paid_orders, 2),              \
                                   '','',''     "

    spark = SparkSession.builder.appName("userid_return_goods_rate").enableHiveSupport().getOrCreate()

    returned_df1 = spark.sql(user_paid_30_orders).cache()
    returned_df1.createTempView("user_paid_30_orders")
    returned_df2 = spark.sql(user_paid_30_return).cache()
    returned_df2.createTempView("user_paid_30_return") 
    spark.sql(user_paid_30_rate)
      
if __name__ == '__main__':
    main()



