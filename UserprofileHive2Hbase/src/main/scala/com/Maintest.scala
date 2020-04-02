package com


object Maintest {
  def main(args: Array[String]): Unit = {
    val smap=scala.collection.SortedMap("3"->"33","2"->"22","1"->"11").
      map(term=>(term._2,term._1)).toList.sortBy(_._2)



    val smap2=scala.collection.SortedMap("3"->"33","2"->"22","1"->"11").
      map(term=>("'" +term._2 + "'","'" +term._1 + "'")).toString()
      .replaceAll(" -> ",":")
//        .replaceAll("Map(","{")
//        .replaceAll(")","}")

    println(s"sting  :${smap2}")

    println("{" + smap2.substring(4,smap2.size-1) + "}")


    val immutableMap = Map("Jim" -> "22", "yxj" -> "32","Jim" -> "", "xx" -> "22000","tt" -> "", "yddxj" -> "7777")
    val sb = new StringBuffer()

    for ((key, value) <- immutableMap){
      sb.append("'" +key + "'" + ":" + "'" + value + "'" + ",")
      println( "'" +key + ":" + "'" + value + "'")
    }

    println("{" + sb.substring(0,sb.length -1) + "}")


    println("==================================")


    val sbkey = new StringBuffer()
    val sbvalue = new StringBuffer()

    for ((key, value) <- immutableMap){
      sbkey.append(key + ":")
      val tagweight = if (value == ""){
        "-999999"
      } else {
        value
      }
      sbvalue.append(tagweight + ":")

    }
    println(sbkey.substring(0,sbkey.length -1))
    println(sbvalue.substring(0,sbvalue.length -1))

  }

}
