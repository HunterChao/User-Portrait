#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import MySQLdb
import commands

def main():
    if len(sys.argv) < 2:
        today = datetime.datetime.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        datestr = yesterday.strftime("%Y%m%d")
        datestr_ = yesterday.strftime("%Y-%m-%d")
    else:
        datestr= sys.argv[1]
        datestr_ = str(datestr)[0:4]+'-'+str(datestr)[4:6]+'-'+str(datestr)[6:8]   #  20190101   2019-01-01
    oneday = datetime.timedelta(days=1)
    daybeforeyesterday = datetime.datetime.strptime(datestr, "%Y%m%d") - oneday
    daybeforeyesterdaystr = daybeforeyesterday.strftime("%Y%m%d")

    
    # 查询 Hive 数据量  os.system("xxxxx")
    r = os.popen("hive -S -e\"select count(1) from dw.dw_userprofile_tag where data_date='"+datestr+"'\"")
    hive_count = r.read()
    r.close()
    hive_count = str(int(hive_count))
    if not hive_count:
        hive_count = -1
    print "hive_result: " + str(hive_count)
    print "Hive select finished!"
    print "###########################"
 
    # hbase 查询 导入Hbase 数据量   userprofile_20190831   userprofile_20190901
    r = os.popen("source /etc/profile; hbase org.apache.hadoop.hbase.mapreduce.RowCounter 'userprofile_"+datestr+"' 2>&1 |grep ROWS")
    hbase_count = r.read().strip()[5:]
    r.close()
    if not hbase_count:
        hbase_count = 0
    print "hbase result: " + str(hbase_count)
    print "HBase select finished!"
    print "###########################"


    # 连接 DB,查前一天Hbase数据，用作数据校验
    db = MySQLdb.connect(host="xx.xx.xx.xx",port=3307,user="username", passwd="password", db="userprofile", charset="utf8")
    cursor = db.cursor()
    selectsql = "SELECT\
                    hbase_count\
                FROM hbase_tag_monitor\
                WHERE date = '" + daybeforeyesterdaystr + "'\
                "
    cursor.execute(selectsql)
    result_tuple = cursor.fetchall()

    if result_tuple:
        hbase_count_yesterday = result_tuple[0][0]
    else:
        hbase_count_yesterday = 0


    # 写同步数据结果到 DB
    try:
        cursor.execute("DELETE FROM hbase_tag_monitor WHERE date='"+datestr_+"' and service_type='addresstag_offline'")
        db.commit()
        cursor.execute("INSERT INTO hbase_tag_monitor(date, service_type, hive_count, hbase_count) VALUES('"+datestr_+"', 'addresstag_offline', "+str(hive_count)+","+str(hbase_count)+")")
        db.commit()
        print "Write addresstag hbase check to DB Finished!"
    except Exception as e:
        db.rollback()
        raise e
        exit(1)
    finally:
        db.close()


if __name__ == '__main__':
    main()



