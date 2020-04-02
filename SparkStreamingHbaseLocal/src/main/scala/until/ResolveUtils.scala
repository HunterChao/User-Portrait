package until

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import scala.collection.mutable.ListBuffer

object ResolveUtils {

  private[this] var industry_category : scala.collection.mutable.ListBuffer[(String, String, String,String)] = ListBuffer()
  private[this] var partner_industry_category : scala.collection.mutable.ListBuffer[(String, String,String, String,String,String)] = ListBuffer()
//  private[this] var  kafkadata : scala.collection.mutable.ListBuffer[(String, String, String)] = ListBuffer()


  // 解析传入json
  def resolvejson(jsondataload: String, industryfileter:String) ={
    val jsondata = jsondataload

    // 身份证号
    val NO_IDCARD = try {JSON.parseObject(jsondata.toString).get("NO_IDCARD")}catch {case ex:Exception => "0"}
    // BE_IDCARD > 0.7
    val BE_IDCARD = try {JSON.parseObject(jsondata.toString).get("BE_IDCARD")}catch {case ex:Exception => "0"}
    // 商户id
    val OID_TRADERNO = try {JSON.parseObject(jsondata.toString).get("OID_TRADERNO")}catch {case ex:Exception => "0"}
    // 设备id
    val DEVICE_ID = try {JSON.parseObject(jsondata.toString).get("DEVICE_ID")}catch {case ex:Exception => "0"}
   // 订单id
    val OID_AUTHORDER = try {JSON.parseObject(jsondata.toString).get("OID_AUTHORDER")}catch {case ex:Exception => "0"}

     val resolveinfo =  NO_IDCARD + "," + BE_IDCARD + "," + OID_TRADERNO + "," + DEVICE_ID + "," + OID_AUTHORDER

    resolveinfo
  }



  // 对MySQL的表注册视图
  def readMYSQLTable(spark: SparkSession, tbName: String, valTableName: String): DataFrame ={
    val quentyDF = spark.read.format("jdbc")
      .option("driver", ParamsUtils.mysql.DB_DRIVER)
      .option("url", ParamsUtils.mysql.DB_URL)
      .option("dbtable", s"${tbName}")
      .option("user", ParamsUtils.mysql.DB_USER)
      .option("password", ParamsUtils.mysql.DB_PASSWORD).load()
    quentyDF.createOrReplaceTempView(valTableName)

    quentyDF

  }

}
