import java.util.Date

object Te {
  def main(args: Array[String]): Unit = {
    val b =  new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2016-06-29 14:50:30.0").getTime
    val kk1 =  new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2016-04-19 00:00:00.0").getTime
    val kk2 =  new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2046-02-18 00:00:00.0").getTime

//   println( (kk2 - kk1)/(365*24*60*60))
//    println( (kk2 - kk1)/(365*24*60*60*1000.0))
//    println( Math.round((kk2 - kk1)/(365.0*24.0*60.0*60.0*1000.0)))
//    89100000/

    val str:String = "苏州市艾美服饰有限公司<2017.01.13>"

    val mm = str.replaceAll("<[0-9]+.[0-9]+.[0-9]+>","")



    val t ="周雪娟(0.01%),李志均(40%)"
    t.indexOf("周雪娟")
    val pattern = "(-?\\d+)(\\.\\d+)?".r
    println((pattern findFirstIn t).toList.sorted.last)

    println(Math.round(84/10.0)*10)
    println(Math.round(85/10.0)*10)
    println(Math.round(86/10.0)*10)
    println(Math.round(94/10.0)*10)
    println(Math.round(95/10.0)*10)
    println(Math.round(96/10.0)*10)

  }
}
