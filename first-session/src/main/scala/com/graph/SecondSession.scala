package com.graph

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import com.utils.DateUtils

object SecondSession {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("FirstSessionAnalysis")
      .config("spark.testing.memory","2147480000")
      .master("local[*]")
      .getOrCreate()

    // 读取原数据 下单用户
    val peopleRDD = spark.sparkContext.textFile("C:\\Users\\hunter\\Desktop\\某电商用户画像系统建设项目实战\\first-session\\cookiesession")
      .map(_.split(","))       // RDD[Array[String]]
      .map( row => Row(row(0),row(1),row(2),row(3),row(4)))    // RDD[Row]

    peopleRDD.persist(StorageLevel.MEMORY_ONLY)

    // 表结构
    val schemas = "cookie,event,ispaid,data_date,time".split(",")
      .map(fp => StructField(fp, StringType))
    val schema = StructType(schemas)

    // 创建视图
    spark.createDataFrame(peopleRDD, schema).createOrReplaceTempView("people_feature")
    spark.sql("select * from people_feature").show(20,50)

    // ---------------------

    val test1 = peopleRDD.map { row => {
      val id = row.getString(0)
      val event = row.getString(1)
      val result = row.getString(2)
      val time = row.getString(3)
      val isOrder = row.getString(4)
      (id, event,result ,time, isOrder)
    }}           // RDD[(String, String, String, String, String)]

    val orderedSessionEventRdd = test1.map { f => (f._1, (f._2, f._3, f._4,f._5)) }
      .groupByKey()
      .mapValues {
        itor => {
          val eventLst = itor.toList.sortBy(f => f._4)
          // 进入日期
          val data_date = eventLst.head._3
          // 进入时间
          val startTime = eventLst.head._4
          // 离开时间
          val endTime = eventLst.last._4
          // 进入事件
          val startevent = eventLst.head._1
          //  离开事件
          val endevent = eventLst.last._1
          // 访问时长
          val start = data_date.toString + " " +startTime.toString
          val end = data_date.toString + " " + endTime.toString
          val visitdiff = DateUtils.timeDiff("2019-01-20 03:22:39", "2019-01-20 03:22:49")
          // 首个Session内是否下单
          val ordered = if (eventLst.head._2 == "NULL") "0" else "1"
          // 访问页面数
          val visitnum = eventLst.length.toString
          // 跳出时间
          val lastEvent = eventLst.last._1
          (eventLst.mkString(""), startTime,endTime,startevent,endevent,ordered,visitnum) // 每个人次数、和时长
        }
      }


    val rowRdd = orderedSessionEventRdd.map(tp => {
      val id = tp._1   // cookie_id
      val eventLst = tp._2._1.mkString("")   // event_cts
      val startTime = tp._2._2
      val endTime = tp._2._3
      val startevent = tp._2._4
      val endevent = tp._2._5
      val ordered = tp._2._6
      val visitnum = tp._2._7
      Row(id, eventLst, startTime, endTime,startevent, endevent, ordered, visitnum)
    })

    val sct = StructType(
      Seq(
        StructField("cookie_id", StringType),
        StructField("eventLst", StringType),
        StructField("startTime", StringType),
        StructField("endTime", StringType),
        StructField("startevent", StringType),
        StructField("endevent", StringType),
        StructField("ordered", StringType),
        StructField("visitnum", StringType)
      ))
    //
    spark.createDataFrame(rowRdd, sct)
      .createOrReplaceTempView("v_tmp_session")

//    spark.sql("select * from v_tmp_session").show(50, 130)
    spark.sql("select * from v_tmp_session").show(20, 130)

    // .select("cookie_id","startTime","endTime").   val rand = new Randow().nextInt(10)

    // 日期  行为事件  人数

    spark.stop()

  }

}
