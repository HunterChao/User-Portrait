#!/usr/bin/env python
# encoding: utf-8

import datetime
import os
import MySQLdb
import sys

# 查询hive中数据
def check_hive_data(data_date):
    hive_user = " select count(1) from dw.es_profile_user_tag_userid where data_date='{}' ".format(data_date)
    hive_cookie = """select count(1) from dw.es_profile_user_tag_cookie where data_date='{}' """.format(data_date)
    user_count = os.popen("hive -S -e \"" + hive_user + "\"").read().strip()
    cookie_count = os.popen("hive -S -e \"" + hive_cookie + "\"").read().strip()

    hive_data = user_count + "," + cookie_count
    return hive_data


# 查询es中数据
def check_es_data(data_date):
    userid_search = "curl http://xx.xx.xx.xxx:9200/_cat/count/" + data_date + "_userid/"
    userid_num = str(os.popen(userid_search).read()).split(' ')[-1].strip()
    cookieid_search = "curl http://xx.xx.xx.xxx:9200/_cat/count/" + data_date + "_cookieid/"
    cookieid_num = str(os.popen(cookieid_search).read()).split(' ')[-1].strip()

    esdata = userid_num + "," + cookieid_num
    return esdata


# 比较hive和es中数据,如通过校验 更新mysql状态位.删除前两日es数据,新建明天es文档大小
def update_es_data(data_date, data_date_, two_day,today):
    esdata = check_es_data(data_date)
    hivedata = check_hive_data(data_date)
    print("esdata ======>{}".format(esdata))
    print("hivedata ======>{}".format(hivedata))

    # 更新mysql状态位
    if ((int(esdata.split(",")[0]) <= int(hivedata.split(",")[0])+10000 and int(esdata.split(",")[0]) >= int(hivedata.split(",")[0])-10000) and \
        (int(esdata.split(",")[1]) <= int(hivedata.split(",")[1])+10000 and int(esdata.split(",")[1]) >= int(hivedata.split(",")[1])-10000)):
        db = MySQLdb.connect(host="xx.xx.xx.xxx", port=3306, user="username", passwd="password",
                             db="userprofile", charset="utf8")
        cursor = db.cursor()
        try:
            select_command = "INSERT INTO `tag_monitor_state` VALUES ('"+ str(data_date_) +"', 'elasticsearch', '0', '2');"
            cursor.execute(select_command)
            db.commit()
        except Exception as e:
            db.rollback()
            exit(1)

        # 设置文档
        set_userid_filerang = """curl -XPUT "http://xx.xx.xx.xxx:9200/""" + str(today) + """_userid" -H 'Content-Type: application/json' -d'{"settings": {"index.mapping.total_fields.limit": 100000000}}'"""
        set_cookieid_filerang = """curl -XPUT "http://xx.xx.xx.xxx:9200/""" + str(today) + """_cookieid" -H 'Content-Type: application/json' -d' {"settings": {"index.mapping.total_fields.limit": 100000000}}'"""
        os.system(set_userid_filerang)
        os.system(set_cookieid_filerang)

        # 删除两日前es数据
        del_userid = """curl -XDELETE "http://xx.xx.xx.xxx:9200/""" + str(two_day) + """_userid" """
        del_cookieid = """curl -XDELETE "http://xx.xx.xx.xxx:9200/""" + str(two_day) + """_cookieid" """
        os.system(del_userid)
        os.system(del_cookieid)
    else:
        print("=============> data syn error")


def main():
    format = "%Y%m%d"
    start_date = sys.argv[1]
    data_date = str(start_date)
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    two_day = strftime(strptime(data_date, format) - datetime.timedelta(2), "%Y%m%d")
    data_date_ = strftime(strptime(data_date, format) - datetime.timedelta(0), "%Y-%m-%d")
    today =  strftime(strptime(data_date, format) + datetime.timedelta(1), format)

    update_es_data(data_date, data_date_, two_day, today)

if __name__ == '__main__':
    main()










