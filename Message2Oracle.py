#!/usr/bin/env python
# encoding: utf-8

import datetime
import os
import csv
import sys
'''
跑完spark任务,将文件从hdfs上拉到 DN3 节点
'''

def get_file_from_hdfs():
    os.system("hadoop fs -get /user/userprofile/system/MessageSystem /home/file/ftpfile")

# 打开本地文件
def get_all_file(data_date):
    filelists = os.listdir("/home/file/ftpfile/MessageSystem")
    for file in filelists:
        file = str(file)
        if file[-4:] == '.csv':
            rename_file(str(file), data_date)


def rename_file(originfilename,data_date):
    with open("/home/file/ftpfile/MessageSystem/" + originfilename, 'r') as file:
        reader = csv.reader(file)
        t = 1
        for i in reader:
            if t == 2:
                tagsys = i[1]
                # 重命名
                rename = "Oracle_Email_User_Info_" + str(data_date) + "_" + str(tagsys).strip('\n') + ".csv"
                oscommond = "mv " + "/home/file/ftpfile/MessageSystem/" + originfilename + " /home/file/ftpfile/MessageSystem/" + rename
                print(oscommond)
                os.system(oscommond)
                # 执行ftp文件传输
                ftp_1 = """sftp -oIdentityFile=/zydata1/responsys/id_rsa.dat  info_scp@files.responsys.net <<EOF
                          cd upload
                          put /home/file/ftpfile/MessageSystem/{}
                          bye
                          EOF""".format(rename)
                os.system(ftp_1)
            t += 1

def main():
    if len(sys.argv) < 2:
        today = datetime.datetime.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        data_date = yesterday.strftime("%Y%m%d")
    else:
        data_date= sys.argv[1]

    # 从hdfs拉到本地
    get_file_from_hdfs()
    # 读取本地路径下的文件
    get_all_file(data_date)

    # 删除HDFS文件 本地数据文件
    os.system("sudo -E -H -u userprofile hadoop fs -rm -r /user/userprofile/system/MessageSystem")
    os.system("rm -rf /home/file/ftpfile/MessageSystem")


if __name__ == '__main__':
    main()


