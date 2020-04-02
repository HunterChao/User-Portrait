package until

import java.sql.DriverManager

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

object SparkUtils{
  def apply(ssc: StreamingContext): SparkUtils = new SparkUtils(ssc)  // 调用class里面的构造器  这个在伴生对象里面才有

  def getStreaming(appName: String): StreamingContext = {
    val conf = new SparkConf().setAppName(appName)
      .set("spark.testing.memory","2147480000")
//      .setMaster("local[*]")
    new StreamingContext(conf, Seconds(2))
  }
}


class SparkUtils(ssc: StreamingContext) {
// 传入ssc 返回线上数据

  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1.toString, offset._2)//topic和分区数
      fromOffsets += (tp -> offset._3)           // offset位置
    }
    fromOffsets
  }

  def getDirectStream (topics: Set[String]):InputDStream[(String,String)] = {
//    val fromOffsetMap =  Map[TopicAndPartition,Long]()

    //  从MySQL读offset信息
    val conn = DriverManager.getConnection(ParamsUtils.mysql.DB_URL, ParamsUtils.mysql.DB_USER, ParamsUtils.mysql.DB_PASSWORD)
    val ps = conn.createStatement()
    val sqlexecute =
      s"""
        | select count(*) as num from offsetinfo where topic = '${ParamsUtils.kafka.KAFKA_TOPIC.mkString(",")}';
      """.stripMargin
    val result = ps.executeQuery(sqlexecute)
    result.next()
    val size = result.getString("num").toInt      // 判断当前topic是否有存储offset

    val ps2 = conn.createStatement()
    val sqlexecute2 =
      """
        | select partitionnum,untiloffset from offsetinfo;
      """.stripMargin
    val result2 = ps2.executeQuery(sqlexecute2)
    var offsetList_ : scala.collection.mutable.ListBuffer[(String, Int, Long)] = ListBuffer()
    while (result2.next()){
      offsetList_.append((ParamsUtils.kafka.KAFKA_TOPIC.mkString(","),result2.getString("partitionnum").toInt,result2.getString("untiloffset").toLong))
    }
   val offsetList =offsetList_.toList         // 获取offset
    conn.close()

//    val offsetList = List((ParamsUtils.kafka.KAFKA_TOPIC.mkString(","), 0, 10.toLong))
    val fromOffsetMap = setFromOffsets(offsetList)       //对List进行处理，变成需要的格式，即Map[TopicAndPartition, Long]
    println(s"fromOffsetMap:    ${fromOffsetMap}")
    println(s"size  ${size}")

    val inputDS : InputDStream[(String, String)] = if (size > 0){
      val messageHandler = (mmd: MessageAndMetadata[String,String]) => (mmd.key, mmd.message)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,(String,String)](
        ssc, ParamsUtils.kafka.KAFKA_PARAMS, fromOffsetMap, messageHandler)
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, ParamsUtils.kafka.KAFKA_PARAMS, ParamsUtils.kafka.KAFKA_TOPIC)
    }

//    不读取offset,直接从最开始消费offset的方案
//    val inputDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//        ssc, ParamsUtils.kafka.KAFKA_PARAMS, ParamsUtils.kafka.KAFKA_TOPIC)
    inputDS
  }

}
