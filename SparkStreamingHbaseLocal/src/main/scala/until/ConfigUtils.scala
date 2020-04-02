package until

import com.typesafe.config.ConfigFactory

object ConfigUtils {
  private val config = ConfigFactory.load()

  // 函数
  private val getString:String => (String) = (str) => config.getString(str)
  private val getInt = (str:String) => config.getInt(str)


  // kafka 集群
  val KAFKA_SERVERS = getString("kafka.servers")
  // kafka 主题
  val KAFKA_TOPIC = getString("kafka.streaming.topic")
  // kafka 分组ID
  val kAFKA_CONSUMER_GROUPID = getString("kafka.consumer.group.id")


  // MySQL驱动
  val DB_DRIVER = getString("db.default.driver")
  // MySQL链接
  val DB_LINK = getString("db.default.url")
  // MySQL用户名
  val DB_USER = getString("db.default.user")
  // MySQL密码
  val DB_PASSWORD = getString("db.default.password")

  // Hbase 集群
  val HBASE_SERVERS = getString("hbase.zookeeper.quorum")
  val HBASE_PARENT = getString("zookeeper.znode.parent")


}
