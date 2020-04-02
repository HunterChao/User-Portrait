# -*- coding: utf-8 -*-
import datetime
import os
import mysql.connector
import sys

def export_data(hive_tab,data_date):
    sqoop_command = "sqoop export --connect jdbc:mysql://10.1.30.150:3306/mysql --username hive " + "--password Ambari123  --table usergroup " + "--export-dir hdfs://newbigdata-6:8020/apps/hive/warehouse/dw.db/" + hive_tab + "/data_date=" + data_date + " --input-fields-terminated-by '\001'"
    os.system(sqoop_command)
    print(sqoop_command)


# 线上状态位   通过状态位 来判断当日数据是否可用
def monitor_state_online(data_date):
    db = mysql.connector.connect(host="10.1.30.150", port=3306, user="hive", passwd="Ambari123", db="mysql", charset="utf8")
    cursor = db.cursor()
    select_command = """select groupid, count(distinct groupuserid) from usergroup where data_date='{}' group by groupid """.format(data_date)
    cursor.execute(select_command)
    data_num = cursor.fetchall()
    print(data_num)
    for i in (1,len(data_num)):
        if len(data_num) == 1:
            insert_command = "insert into usergroup_category(groupid,total,data_date) VALUES({},{},'{}')".format(data_num[i-1][0], data_num[i-1][1],data_date)
            cursor.execute(insert_command)
            db.commit()
            break
        else:
            insert_command = "insert into usergroup_category(groupid,total,data_date) VALUES({},{},'{}')".format(data_num[i-1][0], data_num[i-1][1],data_date)
            print(insert_command)
            cursor.execute(insert_command)
            db.commit()


def main():
    if len(sys.argv) < 2:
        today = datetime.datetime.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        datestr = yesterday.strftime("%Y%m%d")
    else:
        datestr= sys.argv[1]

    export_data("userprofile_tag_usergroup", datestr)
    monitor_state_online(datestr)


if __name__ == "__main__":
    main()



"""
  CREATE TABLE `usergroup` (
  `groupid` varchar(126) NOT NULL,
  `groupuserid` varchar(126) NOT NULL,
  `tagsystem` varchar(126) NOT NULL,
  `reserve` varchar(126) NOT NULL,
  `data_date` varchar(126) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 


  CREATE TABLE `usergroup_category` (
  `groupid` varchar(126) NOT NULL,
  `total` varchar(126) NOT NULL,
  `data_date` varchar(126) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 

"""
