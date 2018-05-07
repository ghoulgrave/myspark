package com.gtmap.spark.etl

import java.io.StringWriter
import java.util.Date

import au.com.bytecode.opencsv.CSVWriter
import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant._
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._


object DataFromFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFromat")
      .setMaster("spark://master:7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)
    val nowTime = getNowDate

    val spark1 = new SQLContext(spark)
    val dy = spark1.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("hdfs://master:9000/Bdcdj/dya")
    val rowsDY: RDD[Row] = dy.rdd
    val dyRdd = rowsDY.keyBy(x => x(9)).map(x => (x._1.toString, (x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7)
      , x._2(8), x._2(10), x._2(11), x._2(12))))

    val xm = spark1.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("hdfs://master:9000/Bdcdj/bdcxm")
    val rowsXM: RDD[Row] = xm.rdd

    val fdcq = spark1.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("hdfs://master:9000/Bdcdj/fdcq")
    val rowsFdcq: RDD[Row] = fdcq.rdd


    //    //proid关联抵押登记结束时间
    val xmRdd = rowsXM.filter(x => x(10) != null).keyBy(x => x(0)).map(x => (x._1.toString, (x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)
      , x._2(9), x._2(10), x._2(11), x._2(12), x._2(13), x._2(14), x._2(15), x._2(16), x._2(17))))
    val dysjRdd = dyRdd.leftOuterJoin(xmRdd)
      .filter(x => x._2._2 != None)
      .filter(x => x._2._1._11 != null && !x._2._1._11.equals("null"))
      .map(x => (x._2._1._1, x._2._1._2, x._2._1._5, x._2._1._6, x._2._1._11, x._2._2.get._7))
    //    //    (bdcdy-76229,320506102085GB00194F00012308,2016-11-11 00:00:00.0,2026-11-11 00:00:00.0,21,2016-12-10 11:13:08.0)
    //    //    (bdcdy-58177,320506001081GB00026F00211104,2014-08-28 00:00:00.0,2044-10-28 00:00:00.0,79,2016-09-08 09:17:13.0)


    //    //bdcdyid分组
    val xmRddByBdcdyid = rowsXM.filter(x => x(10) != null).keyBy(x => x(10)).map(x => (x._2(10), (x._2(0), x._2(1), x._2(2), x._2(3)
      , x._2(4), x._2(5), x._2(6), x._2(7), x._2(8), x._2(9), x._2(10), x._2(11), x._2(12), x._2(13)
      , x._2(14), x._2(15), x._2(16), x._2(17))))
    //
    val xmWithSj = xmRddByBdcdyid.join(dysjRdd.keyBy(x => x._2)).filter(x => x._2._2 != None)
      .filter(x => x._2._1._8 != null && x._2._2._6 != null)
      .filter(x => x._2._1._3 != null && (x._2._1._3.toString.toInt == 4 || x._2._1._3.toString.toInt == 6 || x._2._1._3.toString.toInt == 8))
      .filter(x => x._2._2._3 != null && x._2._2._4 != null)

    val totAllSj = xmWithSj.map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._8, x._2._1._11
      //nd.qlid, nd.BDCDYID, nd.ZWLXKSQX, nd.ZWLXJSQX, nd.BDBZZQSE, nd.BJSJ
      , x._2._2._1, x._2._2._2, x._2._2._3, x._2._2._4, x._2._2._5, x._2._2._6, Math.abs(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._1._8.toString).getTime -
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._2._6.toString).getTime)
    )))
    //    println(totAllSj.count)
    ////    (320506109110GB00025F00300904,(bdcdyzx-TX201706033400,TX201706033400,18,2017-06-13 14:41:19.0,320506109110GB00025F00300904,bdcdy-43278,320506109110GB00025F00300904,2016-06-13 00:00:00.0,2021-06-13 00:00:00.0,111,2016-07-01 09:37:58.0,29999040000))
    ////    (320506109110GB00025F00300904,(bdcdy-43278,201606006-25444,18,2016-07-01 09:37:58.0,320506109110GB00025F00300904,bdcdy-96826,320506109110GB00025F00300904,2017-06-09 00:00:00.0,2022-06-09 00:00:00.0,183.93,2017-06-16 10:38:01.0,30243660000))

    //    //排序并获取最小时间
    val xmMinSj = xmWithSj.map(x => (x._2._1._1,
      Math.abs((new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._1._8.toString).getTime -
        new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._2._6.toString).getTime))
    )).sortBy(x => x._1.toString).groupByKey().map(x => (x._1, x._2.toList.sorted.reverse.last))
    ////    (bdcdy-44722,0)
    ////    (bdcdy-53768,0)
    //    println(xmMinSj.count)

    //项目和抵押信息
    val xmAndDyxx = totAllSj.map(x => (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7
      , x._2._8, x._2._9, x._2._10, x._2._11, x._2._12))
      .keyBy(x => x._1).leftOuterJoin(xmMinSj)
      .map(x => (x._2._1._1,(x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10, x._2._1._11
        , x._2._1._12, x._2._2.toList.sorted.reverse.last)))
      .filter(x => x._2._12 == x._2._13).filter(x => x._2._9 != null)
      //72753
      //    (bdc-792154,201606006059002,4,2016-08-12 15:46:04.0,320506001103GB00112F00020401,bdcdy-49939,320506001103GB00112F00020401,2013-08-14 00:00:00.0,2043-08-14 00:00:00.0,74,2016-08-11 16:18:31.0,84480000,84480000)
      //    (bdc-816412,201606006078634,4,2016-10-12 13:26:29.0,320506130041GB00006F00410806,bdcdy-63522,320506130041GB00006F00410806,2016-09-18 00:00:00.0,2046-09-18 00:00:00.0,85.6,2016-09-30 16:27:02.0,1025940000,1025940000)

      //关联房地产权信息
    val base = xmAndDyxx.join(rowsFdcq.keyBy(x=>x(2)).map(x=>(x._1,(x._2(4)))).filter(x=>x._2 != null && x._2.toString.toDouble >0))
      //proid,(proid,取得价格,抵押结束时间,抵押开始时间,最高额)
      .map(x=> (x._1,(x._1,x._2._2,x._2._1._9,x._2._1._8,x._2._1._10)))
      //proid,(proid,取得价格,抵押时间,最高额)
      .map(x=>(x._1,(x._1,x._2._2.toString.toDouble/10000.0,
        Math.round((new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._3.toString).getTime -
          new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._4.toString).getTime)/(1000*60*60*24*365.0)),
      x._2._5)))
      //proid,(proid,交易价格是否超过评估价格,年还款比例)
      .map(x=> (x._1,(x._1,x._2._2.toDouble / x._2._4.toString.toDouble,x._2._2.toDouble / x._2._3.toDouble / x._2._4.toString.toDouble)))
      //.take(10).foreach(println)


//    val objectEntity = base
//      .map(x => (x._2._1,x._2._2,x._2._3))
//    .repartition(1).saveAsTextFile("hdfs://master:9000/tt/" + "tt"+nowTime)
//

    println("=============================================================================")

    spark.stop()
  }

}
