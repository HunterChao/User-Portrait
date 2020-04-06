package execute

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import until.HbaseUtils
import until.ParamsUtils
import until.ResolveUtils
import until.SparkUtils


object ReadKafka {
  def main(args: Array[String]): Unit = {
    /***
      *  从线上kafka数据拉取注册用户的明细, 统计新注册用户当日累计消费,把累计消费的结果写入hbase
      */
    val sparkConf = new SparkConf().setAppName("SparkStreamingToHbaseLocalMode")
      .set("spark.testing.memory","4294960000")
      .set("spark.streaming.kafka.maxRatePerPartition","200")
      .set("spark.driver.maxResultSize", "6g")
      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      .setMaster("local[*]")

//    val sparkConf = new SparkConf().setAppName("SparkStreaming-tohbase-fileter")
//      .set("spark.streaming.stopGracefullyOnShutdown", "true")
//      .set("spark.streaming.backpressure.enabled","true")
//      .set("spark.streaming.backpressure.initialRate","50")
//       .set("spark.streaming.kafka.maxRatePerPartition","500")
//      .set("spark.streaming.concurrentJobs", "5")
//      .set("spark.dynamicAllocation.enabled", "false")

    val sc = new SparkContext(sparkConf)

   // 过滤id
   val industryfileter_ = HbaseUtils.hbasefilter("DIM_PARTNER_INDUSTRY_MAP",sc)
//val industryfileter_ = "2222,22222222222,333333"
//   println(s"inudstryfilter:  ${industryfileter_}")

    val ssc = new StreamingContext(sc, Seconds(10))
    val messages = new SparkUtils(ssc).getDirectStream(ParamsUtils.kafka.KAFKA_TOPIC)
    println("----------------------  HbaseUtils.loadpartnerhbaseinfo ----------------------")

    // messages 从kafka获取数据,将数据转为RDD
    messages.foreachRDD((rdd, batchTime) => {
      import org.apache.spark.streaming.kafka.HasOffsetRanges
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges   // 获取偏移量信息
      /**
        * OffsetRange 是对topic name，partition id，fromOffset(当前消费的开始偏移)，untilOffset(当前消费的结束偏移)的封装。
        * *  OffsetRange 包含信息有：topic名字，分区Id，开始偏移，结束偏移
        */
      // println("===========================> count: " + rdd.map(x => x + "1").count())
     // offsetRanges.foreach(offset => println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))
      /**
        *  将offset插入MySQL
        */
      // 遍历offsetRanges,里面有多个partition
      for (offset <- offsetRanges) {
        val conn = DriverManager.getConnection(ParamsUtils.mysql.DB_URL, ParamsUtils.mysql.DB_USER, ParamsUtils.mysql.DB_PASSWORD)
        // 存储offset信息, 对于partition新增情况 有就insert 没有就update
        try {
          val sqlexecute =
            s"""
               | INSERT INTO  offsetinfo VALUES ('${offset.topic}' ,'${offset.partition}' , '${offset.untilOffset}') ;
                 """.stripMargin
          val ps = conn.prepareStatement(sqlexecute)
          ps.execute()
          conn.close()
          println(sqlexecute)
        } catch  {
          case exception: Exception =>
            val sqlexecute =
              s"""
                 | UPDATE offsetinfo SET topic = '${offset.topic}' , untiloffset = '${offset.untilOffset}' where partitionnum = '${offset.partition}'  ;
               """.stripMargin
            val ps = conn.prepareStatement(sqlexecute)
            ps.executeUpdate()
            conn.close()
            println(sqlexecute)
        }
      }
    })


    messages.foreachRDD( rdd =>{
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
      val nowtime= dateFormat.format(now)
      val dateFormat3: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val today = dateFormat3.format(now)
      val dateFormat4: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      val today_ = dateFormat4.format(now)

      val industryfileter = if (nowtime >= "00:00:00" & nowtime <= "00:00:30"){
        HbaseUtils.hbasefilter("DIM_PARTNER_INDUSTRY_MAP",sc)
      } else {
        industryfileter_
      }

          // BE_IDCARD > 0.7    NO_IDCARD 不为空   OID_TRADERNO 商户id为互金行业
        rdd.filter( t => ResolveUtils.resolvejson(t._2.toString,industryfileter).split(",")(0) != null)
        .filter(t => ResolveUtils.resolvejson(t._2.toString,industryfileter).split(",")(0) != "null")
        .filter( t => ResolveUtils.resolvejson(t._2.toString,industryfileter).split(",")(1) != "null")
        .filter( t => ResolveUtils.resolvejson(t._2.toString,industryfileter).split(",")(1) > "0.7")
        .filter( t => industryfileter.split(",").contains(ResolveUtils.resolvejson(t._2.toString,industryfileter).split(",")(2)))
     .foreachPartition( partition => {
       val conn = DriverManager.getConnection(ParamsUtils.mysql.DB_URL,ParamsUtils.mysql.DB_USER, ParamsUtils.mysql.DB_PASSWORD)   // mysql
       val prepareStatement = conn.createStatement()
       try {
         partition.foreach(arr => {

           // 处理json,写入MySQL
           val NO_IDCARD = ResolveUtils.resolvejson(arr._2.toString,industryfileter).split(",")(0)
           val DEVICE_ID = ResolveUtils.resolvejson(arr._2.toString,industryfileter).split(",")(3)
           val OID_TRADERNO = ResolveUtils.resolvejson(arr._2.toString,industryfileter).split(",")(2)
           // 订单id  主键
           val OID_AUTHORDER = ResolveUtils.resolvejson(arr._2.toString,industryfileter).split(",")(4)

           val sql = s" REPLACE INTO kafkadata (OID_AUTHORDER, NO_IDCARD, DEVICE_ID, OID_TRADERNO, DATADATE) values ('${OID_AUTHORDER}','${NO_IDCARD}','${DEVICE_ID}','${OID_TRADERNO}','${today}')"
           prepareStatement.addBatch(sql)
         })
       }catch {
         case ex:Exception => "-999999"
       } finally {
         prepareStatement.executeBatch()    // 最后插入MySQL
         conn.close()
       }
     } )

      val conn2 = DriverManager.getConnection(ParamsUtils.mysql.DB_URL,ParamsUtils.mysql.DB_USER, ParamsUtils.mysql.DB_PASSWORD)   // mysql
      val prepareStatement = conn2.createStatement()
      /****
        *  用户汇总指标 从MySQL计算,写入hbase
        */
      val inserthbase = prepareStatement.executeQuery(
        s"""
           |  SELECT NO_IDCARD,
           |         count(distinct OID_AUTHORDER) as SUM_ID_EYE_CPS_CNT_NOW,
           |         count(distinct case when  DEVICE_ID <> 'null' then DEVICE_ID end) as SUM_ID_EYE_CPS_DEV_CNT_NOW,
           |         count(distinct OID_TRADERNO) as SUM_ID_EYE_CPS_TRD_CNT_NOW
           |    FROM kafkadata
           |   WHERE DATADATE = '${today}'
           |GROUP BY NO_IDCARD
            """.stripMargin)
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", ParamsUtils.hbase.HBASE_SERVERS)
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("zookeeper.znode.parent", ParamsUtils.hbase.HBASE_PARENT)
      val connHbase = ConnectionFactory.createConnection(conf)
      val hbasetable = connHbase.getTable(TableName.valueOf(s"EYE_PROFILE_LABEL_REALTIME")) // ORDER_AUTH_EXT
      val hbaseputs = new util.ArrayList[Put]
      try {
        while (inserthbase.next()) {
          val insertinfo = Array("SUM_ID_EYE_CPS_CNT_NOW", "SUM_ID_EYE_CPS_DEV_CNT_NOW", "SUM_ID_EYE_CPS_TRD_CNT_NOW")
          for (i <- 0 to 2) {
            val NO_IDCARD = inserthbase.getString("NO_IDCARD") // kafka中身份证号码
            val SUM_ID_EYE_CPS_CNT_NOW = inserthbase.getString("SUM_ID_EYE_CPS_CNT_NOW")
            val SUM_ID_EYE_CPS_DEV_CNT_NOW = inserthbase.getString("SUM_ID_EYE_CPS_DEV_CNT_NOW")
            val SUM_ID_EYE_CPS_TRD_CNT_NOW = inserthbase.getString("SUM_ID_EYE_CPS_TRD_CNT_NOW")

            val insertdata = SUM_ID_EYE_CPS_CNT_NOW + "," + SUM_ID_EYE_CPS_DEV_CNT_NOW + "," + SUM_ID_EYE_CPS_TRD_CNT_NOW

            val put = new Put(Bytes.toBytes(s"${NO_IDCARD}")) // rowkey
            put.addColumn(Bytes.toBytes("cf"), //family
              Bytes.toBytes(s"${insertinfo(i)}"), // column
              Bytes.toBytes(s"${insertdata.split(",")(i)}")) //value
            hbaseputs.add(put)
          }
        }
      }catch {
        case ex:Exception => "-999999"
      } finally  {
        hbasetable.put(hbaseputs)        // 最后插入hbase
        connHbase.close()
        conn2.close()
      }

    })

    messages.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

