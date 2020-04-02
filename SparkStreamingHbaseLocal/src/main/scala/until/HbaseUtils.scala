package until

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext


object HbaseUtils {
  // 加载历史用户表
  def hbasefilter(tableName: String, sc: SparkContext): String = {

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ParamsUtils.hbase.HBASE_SERVERS)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", ParamsUtils.hbase.HBASE_PARENT)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val hbaseTable = hbaseRDD.map(line=>{
      val hbase_result= line._2
      // 用户id
      val industry_category_code=Bytes.toString(hbase_result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("industry_category_code")))
      // 用户姓名
      val oid_traderno=Bytes.toString(hbase_result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("oid_traderno")))

      (industry_category_code, oid_traderno)
    }).filter( t => "A01001,A01003,A01005,A02003,A02005,A02007,A02008,A06001,A06004,A06007".split(",").contains(t._1) )
      .map( ft => {
        val oid_traderno = ft._2
        (oid_traderno)
      })
        .collect().mkString(",")
    hbaseTable

  }


  def registhbasetable(talbename:String ): (Table, Connection) ={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent","/hbase")
    val connHbase = ConnectionFactory.createConnection(conf)
    val table = connHbase.getTable(TableName.valueOf(s"${talbename}"))

    (table,connHbase)

  }

}
