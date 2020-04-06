package com.graph

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkSql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSql-Analysis")
      .config("spark.testing.memory","2147480000")
      .master("local[*]")
      .getOrCreate()

    // 读取原数据 下单用户
    val peopleRDD = spark.sparkContext.textFile("C:\\Users\\hunter\\Desktop\\某电商用户画像系统建设项目实战\\first-session\\activedCookieIdUserGroupDF.txt")
      .map(_.split(","))       // RDD[Array[String]]
      .map( row => Row(row(0),row(1)))    // RDD[Row]
    // 表结构

    val schemas = "cookie,tagsmap".split(",")
      .map(fp => StructField(fp, StringType))
    val schema = StructType(schemas)

    // 创建视图
    spark.createDataFrame(peopleRDD, schema).createOrReplaceTempView("people_feature")
    spark.sql("select * from people_feature").show(20,140)
    println(spark.createDataFrame(peopleRDD, schema).count())


    spark.createDataFrame(peopleRDD, schema).rdd.map{
      row =>{
       val id = row.getAs[String]("cookie")
       val tags = row.getAs[Map[String,String]]("tagsmap").mapValues( f=> if (f==null) "-999999" else f)
       if (tags.size>100) {
         ("-1", Map("-1" -> "-1"))
       }else{
         (id,tags)
       }
        (id, tags)
      }
    }.map(x => (x, 1)).reduceByKey(_ + _)            //.coalesce(10,shuffle = true)


    val textFile  = spark.sparkContext.textFile("C:\\Users\\king\\Desktop\\first-session\\activedCookieIdUserGroupDF.txt")
    val counts = textFile.flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .map( line =>{
        val cookieid = line._1.toString
        val cnt = line._2.toString
        Row(cookieid,cnt)
      })

    spark.createDataFrame(counts, schema).createOrReplaceTempView("people_act_counts")
    spark.sql("select * from people_act_counts").show(20,140)


  }

}
