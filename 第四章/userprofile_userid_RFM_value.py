#!/usr/bin/env python
# encoding: utf-8

"""
File: userprofile_userid_RFM_value.py
Date: 2018/10/01 
submit command: 
spark-submit --master yarn --deploy-mode client --driver-memory 1g  --executor-memory 2g 
--executor-cores 2 --num-executors 50 userprofile_userid_RFM_value.py start-date
"""

# A111U008_001    重要价值用户
# A111U008_002    重要保持用户
# A111U008_003    重要发展用户
# A111U008_004    重要挽留用户
# A111U008_005    一般价值用户
# A111U008_006    一般保持用户
# A111U008_007    一般发展用户
# A111U008_008    一般挽留用户
# dim_user_info
# user_consume_info

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import sys
import datetime

def main():
    start_date = sys.argv[1]
    start_date_str = str(start_date)
    date_str = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1),'%Y-%m-%d')
    format = "%Y%m%d"
    target_table = 'dw.profile_tag_user' 

    
    # 用户RFM维度数据 (用户最后一次购买时间非空)
    user_rfm_info =  "  select t1.user_id,                                                                     \
                               t2.country,                                                                     \
                               t1.last_1y_paid_orders,                                                         \
                               t1.last_1y_paid_order_amount,                                                   \
                               concat(substr(t1.last_order_paid_time,1,4),'-',substr(t1.last_order_paid_time,5,2),'-',substr(t1.last_order_paid_time,7,2)) as last_pay_date  \
                          from dw.user_consume_info t1                                                                     \
                     left join (                                                                                              \
                                 select user_id,                                                                              \
                                        country                                                                               \
                                   from (                                                                                     \
                                         select user_id,                                                                      \
                                                country,                                                                      \
                                                row_number() over(partition by user_id order by last_date desc) as rank       \
                                           from dim.dim_user_info                                                             \
                                          where data_date ="+"'"+start_date_str+"'"+"                                         \
                                            and site_id in (600,900)                                                          \
                                            and country is not null                                                           \
                                        ) t                                                                                   \
                                   where t.rank =1                                                                            \
                                 )  t2                                                                                        \
                            on t1.user_id = t2.user_id                                                                        \
                         where t1.data_date = "+"'"+start_date_str+"'"+"                                                      \
                           and t1.last_order_paid_time is not null                                                            \
                           and t1.app_name = 'JC' "
   
   
    user_rfm = "   select user_id,                                                                 \
                          case when datediff("+"'"+date_str+"'"+",last_pay_date)<90 then '近'      \
                          else '远' end as date_diff,                                              \
                          case when last_1y_paid_orders <3 then '低频'                             \
                          else '高频' end as sum_orders,                                           \
                          case when last_1y_paid_order_amount <50 then '低额'                      \
                          else '高额' end as sum_amount                                            \
                     from user_rfm_info                                                            \
                    where country = 'ID'                                                           \
                union all                                                                          \
                    select user_id,                                                                \
                          case when datediff("+"'"+date_str+"'"+",last_pay_date)<90 then '近'      \
                          else '远' end as date_diff,                                              \
                          case when last_1y_paid_orders <3 then '低频'                             \
                          else '高频' end as sum_orders,                                           \
                          case when last_1y_paid_order_amount <300 then '低额'                     \
                          else '高额' end as sum_amount                                            \
                     from user_rfm_info                                                            \
                    where country <> 'ID'    "


    insert_table = "insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='rfm_model')    \
                            select case when date_diff = '近' and sum_orders = '高频' and sum_amount = '高额' then 'A111U008_001'    \
                                        when date_diff = '远' and sum_orders = '高频' and sum_amount = '高额' then 'A111U008_002'    \
                                        when date_diff = '近' and sum_orders = '低频' and sum_amount = '高额' then 'A111U008_003'    \
                                        when date_diff = '远' and sum_orders = '低频' and sum_amount = '高额' then 'A111U008_004'    \
                                        when date_diff = '近' and sum_orders = '高频' and sum_amount = '低额' then 'A111U008_005'    \
                                        when date_diff = '远' and sum_orders = '高频' and sum_amount = '低额' then 'A111U008_006'    \
                                        when date_diff = '近' and sum_orders = '低频' and sum_amount = '低额' then 'A111U008_007'    \
                                        else 'A111U008_008' end as tagid,                                                             \
                                   user_id as userid,           \
                                   '' as tagweight,             \
                                   '' as tagranking,            \
                                   '' as reserve,               \
                                   '' as reserve1               \
                              from user_rfm "
                     
                     
    spark = SparkSession.builder.appName("user_rfm_model").enableHiveSupport().getOrCreate()
    returned_df1 = spark.sql(user_rfm_info).cache()
    returned_df1.createTempView("user_rfm_info")
    returned_df2 = spark.sql(user_rfm).cache()
    returned_df2.createTempView("user_rfm")
    
    spark.sql(insert_table)
      
if __name__ == '__main__':
    main()

