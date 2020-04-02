#!/usr/bin/env python
# encoding: utf-8

import datetime
import os
import sys

def ToHfile(data_date):
    # 判断活跃节点
    global activenode
    for node in ("10.151.15.199","10.151.15.200"):
        command = "curl http://"+ str(node) + ":9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
        status = os.popen(command).read()
        print("Hbase Master status: ".format(status))
        if ("active" in status):
            activenode = node

    # 起spark任务 将数据写入Hfile
    HfileInfo= "hadoop fs -ls hdfs://" + str(activenode) + ":8020/user/bulkload/hfile/eda_usertags_" + str(data_date)
    if ("Found" in os.popen(HfileInfo).read()):
        os.system("hadoop fs -rm -r hdfs://" + str(activenode) + ":8020/user/bulkload/hfile/eda_usertags_" + str(data_date))  # 删除Hfile
        sparkcommand = "sudo -E -H -u hdfs spark-submit --master yarn --deploy-mode client --queue root.core.app --driver-memory 2g \
              --executor-memory 8g --num-executors 50 --executor-cores 2 --class com.Hive2HbasePre  /data1/jar/userprofile/usergroup/UserprofileHive2Hbase-jar-with-dependencies.jar {} {}".format(str(data_date), str(activenode))
        os.system(sparkcommand)
    else:
        sparkcommand = "sudo -E -H -u hdfs spark-submit --master yarn --deploy-mode client --queue root.core.app --driver-memory 2g \
                      --executor-memory 8g --num-executors 50 --executor-cores 2 --class com.Hive2HbasePre  /data1/jar/userprofile/usergroup/UserprofileHive2Hbase-jar-with-dependencies.jar {} {}".format(str(data_date), str(activenode))
        os.system(sparkcommand)

    #  把Hfile bulkload到hbase
    Hfile2Hbase(activenode, data_date)


def Hfile2Hbase(node,data_date):
    # 创建Hbase预分区表
    hbase_table = "ILAMBDARS.USERCOOKIELABEL.ILAMBDARSUSERCOOKIELABEL." + str(data_date)
    createhbase = """ssh ilambda@bi-sp32-west4-b  hbase --config /etc/hbase/conf shell << TTT create '{}', { NAME => "cf", BLOCKCACHE => "true" , BLOOMFILTER => "ROWCOL" ,  COMPRESSION => 'snappy', IN_MEMORY => 'true' }, {NUMREGIONS => 10,SPLITALGO => 'HexStringSplit'} TTT""".format(hbase_table)
    os.system(createhbase)

    # 把Hfile bulkload到hbase
    # step1
    step1 = "ssh ilambda@bi-sp32-west4-b hadoop fs -get hdfs://" + str(node) + ":8020/user/bulkload/hfile/eda_usertags_" + str(data_date)
    os.system(step1)
    # step2
    step2 ="ssh ilambda@bi-sp32-west4-b hadoop fs -put eda_usertags_" + str(data_date) + "/user/ilambda/data"
    os.system(step2)
    # step3
    synchbase = "ssh ilambda@bi-sp32-west4-b hbase --config /etc/hbase/conf org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles  \
                  /user/ilambda/data/eda_usertags_{} {}".format(str(data_date), hbase_table)
    os.system(synchbase)

    # 清除Hfile
    os.system("ssh ilambda@bi-sp32-west4-b  rm -r eda_usertags_{}" + str(data_date))

def main():
    if len(sys.argv) < 2:
        today = datetime.datetime.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        data_date = yesterday.strftime("%Y%m%d")
    else:
        data_date= sys.argv[1]
    ToHfile(data_date)

if __name__ == '__main__':
    main()

