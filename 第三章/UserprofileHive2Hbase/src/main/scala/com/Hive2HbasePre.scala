package com

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

object Hive2HbasePre {

  def nullHandle(str: String):String = {
    if (str == null || "".equals(str)) {
      return "NULL"
    } else {
      return str
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 2) {
      System.err.println(
        """
          | Usage: UserGroupFilterTask  must input four parameter
          | 1st  is  a tableName for save the usegroup data
          | 2st  is  a target_system for push data to system
        """.stripMargin)
      System.exit(1)
    }
    val data_date = args(0)
    val node = args(1)

    val spark = SparkSession
      .builder()
      .appName("Hive2Hbase")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.storage.memoryFraction", "0.1")
      .config("spark.shuffle.memoryFraction", "0.7")
      .config("spark.memory.useLegacyMode", "true")
      .enableHiveSupport()
      .getOrCreate()

    val Data = spark.sql(s"select cookieid,data_date,reserve,tagsmap from  dw.dw_profile_user_tag_cookie_online where data_date = '${data_date}'  ")
    val dataRdd = Data.rdd.flatMap(row => {     //cf是列族名,ID、DATA_TYPE、DEVICE_ID为字段名
      val rowkey = row.getAs[String]("cookieid".toLowerCase)
      val data_date = nullHandle(row.getAs[String]("data_date".toLowerCase))
      val reserve = nullHandle(row.getAs[String]("reserve".toLowerCase))
      val tagsmap = row.getAs[Map[String, Object]]("tagsmap".toLowerCase)

      val sb = new StringBuffer()    // 对MAP结构转化 a->b  'a':'b'
      for ((key, value) <- tagsmap){
        sb.append("'" +key + "'" + ":" + "'" + value + "'" + ",")
      }
      val tagsmaps = "{" + sb.substring(0,sb.length -1) + "}"
      Array(
        (rowkey,("cf","data_date",data_date)),
        (rowkey,("cf","reserve",reserve)),
        (rowkey,("cf","tagsmap",tagsmaps))
      )
    })

    //要保证行键，列族，列名的整体有序，必须先排序后处理,防止数据异常过滤rowkey
    val rdds = dataRdd.filter(x=>x._1 != null).sortBy(x=>(x._1,x._2._1,x._2._2)).map(x => {
      //将rdd转换成HFile需要的格式,Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
      //KeyValue的实例为value
      val rowKey = Bytes.toBytes(x._1)
      val family = Bytes.toBytes(x._2._1)
      val colum = Bytes.toBytes(x._2._2)
      val value = Bytes.toBytes(x._2._3.toString)
      (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, value))
    })

    //临时文件保存位置，在hdfs上
    val tmpdir = "hdfs://" + node.toString + ":8020/user/bulkload/hfile/eda_usertags_" + data_date

    val hconf = new Configuration()
    hconf.set("fs.defaultFS", s"hdfs://${node.toString}/")

    val fs = FileSystem.get(new URI(s"hdfs://${node.toString}"), hconf, "jollybi") //hadoop为你的服务器用户名

    if (fs.exists(new Path(tmpdir))){    //由于生成Hfile文件的目录必须是不存在的，所以我们存在的话就把它删除掉
      println("删除临时文件夹")
      fs.delete(new Path(tmpdir), true)
    }
    //创建HBase的配置
    val conf = HBaseConfiguration.create()
    //      conf.set("hbase.zookeeper.quorum", "10.142.170.209,10.142.170.251")
    //      conf.set("hbase.zookeeper.property.clientPort", "8020")

    //为了预防hfile文件数过多无法进行导入，设置该参数值
    //      conf.setInt("hbase.hregion.max.filesize", 10737418240)
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3200)

    //此处运行完成之后,在tmpdir生成的Hfile文件
    rdds.saveAsNewAPIHadoopFile(tmpdir,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    spark.close()

  }
}
