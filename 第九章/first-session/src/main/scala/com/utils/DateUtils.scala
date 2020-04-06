package com.utils

import java.text.SimpleDateFormat

object DateUtils {


  /**
    * 返回 时间差
    * 单位 s/秒
    *
    * @param start
    * @param end
    * @return
    */
  def timeDiff(start: String, end: String): Long = {
    var diffSec = 0L
    if (isEmpty(start) || isEmpty(end)) return diffSec
    try {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val st = df.parse(start.toString)
      val et = df.parse(end.toString)

      val diff = et.getTime - st.getTime
      diffSec = diff / 1000
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    diffSec
  }


  def main(args: Array[String]): Unit = {

    val l = timeDiff("2019-01-20 03:22:39", "2019-01-20 03:22:49")

    println(l)

  }

  def isEmpty(s: String): Boolean = s == null || "".equals(s)

}
