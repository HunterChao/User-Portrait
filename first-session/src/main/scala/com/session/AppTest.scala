package com.session

import com.utils.DateUtils

object AppTest {

  def main(args: Array[String]): Unit = {

    val lst = List(("1", ("key_checkout_result", "success")),
      ("2", ("key", "fail")),
      ("1", ("key_checkout_result", "success"))
    )
    val map: Map[String, List[(String, (String, String))]] = lst.groupBy(f => f._1)
    println(lst.groupBy(f => f._2))


    map.map(f=>{
      val lst = f._2
      println(lst)
      map.map(tp=> {
        println(tp._2.map(l=> l._2._1+"$"+l._2._2).contains("key_checkout_result$success"))
      })
    })

    println("====================================================")
   val test1 = List(("0000DB4A-615C-4A63-B6D0-8213BDC956F7","loginreg_phonenumber_next_result","2019-01-20 17:49:09","2019-01-20 18:49:09"),
     ("0000DB4A-615C-4A63-B6D0-8213BDC956F7","message_click","17:53:19","17:57:19"),
     ("00014e16-ce85-4c4a-b511-001cc50dc156","loginreg_phonenumber_region_click","2019-01-20 03:41:37","2019-01-20 17:53:19"))

   println(test1.groupBy(f => f._1)
       .mapValues{ itor => {
         val eventLst = itor.toSeq.toList.map(_._2).mkString("")
         val start = itor.map(_._3).mkString("")
         val end = itor.map(_._4).mkString("")
         val diff = DateUtils.timeDiff(start.toString, end.toString)

         (eventLst, start, end, diff)
       }}
   )  //map.(f => (f._1,f._2))

    val testa = List("key_app_open", "loginreg_view", "loginreg_phonenumber_next_click", "loginreg_phonenumber_next_click")


    println(s"testa:  '${testa(0)}', '${testa(1)}',  '${testa(2)}' ")

  }
}
