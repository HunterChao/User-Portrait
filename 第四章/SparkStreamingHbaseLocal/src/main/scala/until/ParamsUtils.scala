package until

object ParamsUtils {

  // kafka配置参数
  object kafka {
    val KAFKA_PARAMS = Map[String, String](
      "metadata.broker.list" -> ConfigUtils.KAFKA_SERVERS,
      "group.id" -> ConfigUtils.kAFKA_CONSUMER_GROUPID,
      "auto.commit.enable" -> "false",
      "auto.offset.reset" -> "smallest",
      "fetch.message.max.bytes"->"10485760")
    val KAFKA_TOPIC = ConfigUtils.KAFKA_TOPIC.split(",").toSet
  }
// largest    smallest

  // mysql配置参数
  object mysql{
      val DB_URL = ConfigUtils.DB_LINK
      val DB_USER = ConfigUtils.DB_USER
      val DB_DRIVER = ConfigUtils.DB_DRIVER
      val DB_PASSWORD = ConfigUtils.DB_PASSWORD
}

  object hbase{
    val HBASE_SERVERS = ConfigUtils.HBASE_SERVERS
    val HBASE_PARENT = ConfigUtils.HBASE_PARENT
  }


}
