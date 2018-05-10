package com.gtmap.spark.etl

import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant.getNowDate
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

object DateOfPersion {

  def main(args: Array[String]): Unit = {
//    base:68510
//    (bdc-834085,(1,bdc-834085,0.23452264150943397,0.011726132075471699))
//    (bdc-752891,(0,bdc-752891,1.56334,0.052111333333333336))
//    (bdc-783453,(0,bdc-783453,1.4343261205564142,0.055166389252169774))
//    (bdc-835413,(0,bdc-835413,1.2893939393939393,0.10744949494949493))
//    (bdc-921161,(0,bdc-921161,1.390549315068493,0.06952746575342465))
//    (bdc-843175,(0,bdc-843175,1.664705376344086,0.0832352688172043))
//    (bdc-852056,(0,bdc-852056,1.417187857961054,0.0708593928980527))
//    (bdc-918950,(0,bdc-918950,2.1145414285714286,0.07551933673469387))
//    (bdc-830728,(0,bdc-830728,1.4293478260869565,0.04764492753623189))
//    (bdc-814539,(0,bdc-814539,1.4291044776119404,0.047636815920398014))

    val conf = new SparkConf().setAppName("DataFromat")
      .setMaster("spark://master:7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)
    val nowTime = getNowDate
    val spark1 = new SQLContext(spark)

    val qlr = spark1.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("hdfs://master:9000/Bdcdj/qlrxx")
    val rowsDY: RDD[Row] = qlr.rdd

    val qlrRdd = rowsDY
      .filter(x =>x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0").filter(x=> x(4) == "qlr")
      .keyBy(x => x(3))
      .map(x=>(x._1,(x._2(1),x._2(3),x._2(2).toString.replaceAll("<[0-9]+.[0-9]+.[0-9]+>",""))))
      .map(x=>(x._2._1,x._1+"^"+x._2._3))
//      println(qlrRdd.count)
//      qlrRdd.take(10).foreach(println)
//    286480
//    (bdc-801674,513023197104056746熊德蓉)
//    (bdc-801973,320524196401137017杭火林)
//    (bdc-802790,320681198910090829陈晓明)


    val fdcq = spark1.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("hdfs://master:9000/Bdcdj/fdcq")
    val rowsFdcq: RDD[Row] = fdcq.rdd

    val fdcqRdd = rowsFdcq.keyBy(x=>x(2)).filter(x=> x._2(5) != null).filter(x=> x._2(5).toString.toInt == 1)
      .map(x => (x._1,x._2(1)))
//      println(fdcqRdd.count)
//      fdcqRdd.take(10).foreach(println)
//    186301
//    (bdcsc-865429,320506104121GB00022F00120906)
//    (bdcsc-863899,320506014003GB00013F00012092)
//    (bdcsc-864088,320506014003GB00013F00011035)

    val qlrAndBdcdy = fdcqRdd.leftOuterJoin(qlrRdd)
      .filter(x=>x._2._2 != None)
        .map(x=>(x._2._2.toList.sorted.last,1))
      .reduceByKey(_+_)
//    println(qlrAndBdcdy.count)
//    qlrAndBdcdy.take(10).foreach(println)
//    153106
//    (220222197310290034^朴铉哲,1)
//    (342822197103305119^胡少根,2)
//    (370982198207216874^刘传新,1)

    val qlrAndBdcdyAndProid= qlrRdd.keyBy(x=>x._2).leftOuterJoin(qlrAndBdcdy)
      .filter(x=> x._2._2 != None)
        .map(x=>(x._2._1._1,x._2._2.toList.sorted.last))
//      .map(x => x._2)

        println(qlrAndBdcdyAndProid.count)
        qlrAndBdcdyAndProid.take(10).foreach(println)
//    259122
//    (220222197310290034^朴铉哲,((bdc-908423,220222197310290034^朴铉哲),Some(1)))
//    (370982198207216874^刘传新,((bdc-805512,370982198207216874^刘传新),Some(1)))
//    (342822197103305119^胡少根,((bdc-795103,342822197103305119^胡少根),Some(2)))
//    (342822197103305119^胡少根,((bdc-783723,342822197103305119^胡少根),Some(2)))







  }

}
//178136