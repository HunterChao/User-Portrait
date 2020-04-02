#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
import sys
import datetime


if len(sys.argv) < 2:
    today = datetime.datetime.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    datestr = yesterday.strftime("%Y%m%d")
else:
    datestr= sys.argv[1]


def funflatmap(userid,mapstr,dau):
    res=[]
    dautag=1
    if dau is None:
        dautag=0
    for tag in mapstr[1:-1].split(","):
        tempstr=(tag.strip()[2:-1],1)
        tempstr1=("dau-"+tag.strip()[2:-1],dautag)
        res.append(tempstr)
        res.append(tempstr1)
    return res


def main():
    if len(sys.argv) < 2:
        today = datetime.datetime.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        datestr = yesterday.strftime("%Y%m%d")
    else:
        datestr= sys.argv[1]
    db = MySQLdb.connect(host="xx.xx.xx.142",port=3307,user="userprofile", passwd="password", db="userprofile", charset="utf8")
    cursor = db.cursor()
    resdict={}
    print "data=========="+datestr
    spark = SparkSession.builder.appName("checkdata_userid").enableHiveSupport().getOrCreate()
    dosql="select * from (select userid,tagsmap from dw.dw_profile_user_tag where data_date='"+datestr+"') ta left outer join (select userid as user_id  from dw.dw_cookie_user_login where data_date='"+datestr+"' group by userid) tb on ta.userid = tb.user_id"
    temprdd=spark.sql(dosql).rdd.cache()
    # 计算用户总人数
    tagusertotal=temprdd.map(lambda r:(r.userid, r.user_id)).reduceByKey(lambda x, y: x).count()
    # 计算dau覆盖率
    daucountsql="select userid as user_id  from dw.dw_cookie_user_login where data_date='"+datestr+"') group by userid"
    daucount=spark.sql(daucountsql).count()
     
    res=temprdd.flatMap(lambda x:funflatmap(x.userid,str(x.tagsmap.keys()),x.user_id)).reduceByKey(lambda a, b: a + b).collect()
    for line in res:
        if line[0][0:3]=="dau":
            tagkey=line[0][4:]
            if resdict.has_key(tagkey):
                resdict[tagkey]=(resdict[tagkey][0],resdict[tagkey][1],float(line[1])/float(daucount))
            else:
                resdict[tagkey]=(0,0,float(line[1])/float(daucount))
        else:
            if resdict.has_key(line[0]):
                resdict[line[0]]=(line[1],float(line[1])/float(tagusertotal),resdict[line[0]][2])
            else:
                resdict[line[0]]=(line[1],float(line[1])/float(tagusertotal),0)
    for key in resdict.keys():
        desql="DELETE FROM tag_num where tagid='"+key+"' and date='"+datestr+"'"
        sql="INSERT INTO tag_num(tagid,date,tag_count,tag_total_per,tag_dau_per) VALUES('"+key+"','"+datestr+"',"+str(resdict[key][0])+","+str(resdict[key][1])+","+str(resdict[key][2])+")"
        try:
            cursor.execute(desql)
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
    db.close()
    
if __name__== '__main__':
    main()




