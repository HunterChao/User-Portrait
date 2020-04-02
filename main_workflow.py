#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import datetime
import os

if len(sys.argv) < 2:
    today = datetime.datetime.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    datestr = yesterday.strftime("%Y%m%d")
else:
    datestr= sys.argv[1] 

os.system("export PYTHONIOENCODING=utf8")
os.system("export SPARK_HOME=/usr/local/spark-2.1.1-bin-hadoop2.6")
os.system("export JAVA_HOME=/usr/local/jdk1.8.0_162/")
os.system("export PATH=$JAVA_HOME/bin:$PATH")

os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client  --queue root.production.userprofile  --driver-memory 1g  --executor-memory 8g --executor-cores 2 --num-executors 50  /home/userprofile/userprofile_cookieid_gender.py " + datestr)
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client  --queue root.production.userprofile --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /home/userprofile/userprofile_cookieid_country.py  " + datestr)
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client  --queue root.production.userprofile --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /home/userprofile/userprofile_cookieid_install_days.py  " + datestr)
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client  --queue root.production.userprofile --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /home/userprofile/userprofile_cookieid_last_paid_days.py  " + datestr)
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client  --queue root.production.userprofile --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /home/userprofile/userprofile_cookieid_purchase_state.py  " + datestr)
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client  --queue root.production.userprofile --driver-memory 1g  --executor-memory 4g --executor-cores 2 --num-executors 50  /home/userprofile/userprofile_cookieid_registed_state.py  " + datestr)
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client  --queue root.production.userprofile --driver-memory 4g  --executor-memory 8g --executor-cores 2 --num-executors 50  /home/userprofile/userprofile_userid_edm.py " + datestr)

# 校验标签
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client --queue root.production.userprofile --driver-memory 4g --executor-memory 8g --executor-cores 2 --num-executors 50  /home/userprofile/checkdata_cookieid.py " + datestr)
os.system("/usr/local/spark-2.1.1-bin-hadoop2.6/bin/spark-submit   --master yarn --deploy-mode client --queue root.production.userprofile --driver-memory 4g  --executor-memory 8g --executor-cores 2 --num-executors 50  /home/userprofile/checkdata_userid.py " + datestr)



