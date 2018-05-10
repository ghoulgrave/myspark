package com.gtmap.spark.etl


import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant._
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
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
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._2._6.toString).getTime), x._2._2._1
    )))
    //    println(totAllSj.count)
    //    (320506001117GB00227F02690702,(bdc-772198,201606006040617,4,2016-09-14 10:07:16.0,320506001117GB00227F02690702,bdcdy-57595,320506001117GB00227F02690702,2014-11-30 00:00:00.0,2044-11-30 00:00:00.0,60,2016-09-06 12:03:47.0,684209000,bdcdy-57595))
    //    (320506109097GB00005F00101804,(bdc-849299,201606006101139,4,2017-01-17 13:12:15.0,320506109097GB00005F00101804,bdcdy-80785,320506109097GB00005F00101804,2015-05-18 00:00:00.0,2035-05-17 00:00:00.0,68,2016-12-28 09:56:02.0,1739773000,bdcdy-80785))

    //==============
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
      , x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13))
      .keyBy(x => x._1).leftOuterJoin(xmMinSj)
      .map(x => (x._2._1._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10, x._2._1._11
        , x._2._1._12, x._2._2.toList.sorted.reverse.last, x._2._1._13)))
      .filter(x => x._2._12 == x._2._13).filter(x => x._2._9 != null)
    //        .take(10).foreach(println)
    //72753
    //    (bdc-752891,(bdc-752891,201606006024785,4,2016-08-09 09:18:09.0,320506432035GB00103F00900504,bdcdy-45574,320506432035GB00103F00900504,2014-05-14 00:00:00.0,2044-05-14 00:00:00.0,100,2016-07-18 10:15:53.0,1897336000,1897336000,bdcdy-45574))
    //    (bdc-733278,(bdc-733278,201606006012291,4,2016-03-24 09:17:08.0,320506129006GB00340F00050506,bdcdy-36193,320506129006GB00340F00050506,2016-04-19 00:00:00.0,2046-04-18 00:00:00.0,87,2016-05-11 14:45:30.0,4166902000,4166902000,bdcdy-36193))

    //关联房地产权信息
    val base = xmAndDyxx.join(rowsFdcq.keyBy(x => x(2)).map(x => (x._1, (x._2(4)))).filter(x => x._2 != null && x._2.toString.toDouble > 0))
      //proid,(proid,取得价格,抵押结束时间,抵押开始时间,最高额,dyproid,bdcdyid)
      .map(x => (x._1, (x._1, x._2._2, x._2._1._9, x._2._1._8, x._2._1._10, x._2._1._14, x._2._1._5)))
      //proid,(proid,取得价格,抵押时间,最高额,dyproid,bdcdyid)
      .map(x => (x._1, (x._1, x._2._2.toString.toDouble / 10000.0,
      Math.round((new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._3.toString).getTime -
        new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._4.toString).getTime) / (1000 * 60 * 60 * 24 * 365.0)),
      x._2._5, x._2._6, x._2._7)))
      //proid,(proid,交易价格是否超过评估价格,年还款比例,dyproid,bdcdyid)
      .map(x => (x._1, (x._1, x._2._2.toDouble / x._2._4.toString.toDouble, x._2._2.toDouble / x._2._3.toDouble / x._2._4.toString.toDouble, x._2._5, x._2._6)))
    //          .take(10).foreach(println)
    //    (bdc-793805,(bdc-793805,1.3432530864197532,0.13432530864197534,bdcdy-50915,320506129043GB00325F00030330))
    //    (bdc-921161,(bdc-921161,0.9270328767123287,0.046351643835616435,bdcdy-112918,320506102080GB00156F00262108))
    //    (bdc-854898,(bdc-854898,0.9554367201426025,0.034122740005092946,bdcdy-82837,320506432105GB00032F00050601))


    val cf = spark1.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("hdfs://master:9000/Bdcdj/cf")
    val rowsCf: RDD[Row] = cf.rdd
    //
    val cfByBdcdy = rowsCf.keyBy(x => x(1)).map(x => (x._1, 1))
    //        //.take(10).foreach(println)
    //
    val xmByBdcdy = rowsXM.filter(x => x(10) != null).keyBy(x => x(10)).map(x => (x._1, (x._2(0))))
    ////      .take(10).foreach(println)
    //
    val bdccf = xmByBdcdy.leftOuterJoin(cfByBdcdy).map(x => {
      if (x._2._2 == None) {
        (x._2._1, 0)
      } else {
        (x._2._1, 1)
      }
    }).distinct()

    val baseANDCf = base.leftOuterJoin(bdccf)
      //      //(proid,(是否有查封,proid,交易价格是否超过评估价格,年还款比例))
      .map(x => (x._1, (x._2._2.toList.sorted.last, x._2._1._1, x._2._1._5, x._2._1._2, x._2._1._3, x._2._1._4.toString)))
    //      .take(10).foreach(println)
    //    (bdc-918950,(0,bdc-918950,320506109110GB00175F00691703,1.4096942857142858,0.050346224489795914,bdcdy-112453))
    //    (bdc-830728,(0,bdc-830728,320506109091GB00068F00020801,0.9528985507246377,0.03176328502415459,bdcdy-71941))
    //    (bdc-814539,(0,bdc-814539,320506109033GB00015F00100301,0.9527363184079602,0.03175787728026534,bdcdy-69312))

    val qlr = spark1.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("hdfs://master:9000/Bdcdj/qlrxx")
    val rowsQlr: RDD[Row] = qlr.rdd
    //
    val qlrRdd = rowsQlr
      .filter(x => x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0").filter(x => x(4) == "ywr")
      .keyBy(x => x(3))
      .map(x => (x._1, (x._2(1), x._2(3), x._2(2).toString.replaceAll("<[0-9]+.[0-9]+.[0-9]+>", ""))))
    //      .take(10).foreach(println)
    //    (91320507762402583X,(bdc-870691,91320507762402583X,苏州新中天置业有限公司))
    //    (91320507762402583X,(bdc-870692,91320507762402583X,苏州新中天置业有限公司))
    //    (113205060141960080,(bdc-871973,113205060141960080,苏州市吴中区人民政府香山街道办事处))

    val qlrRddCount = qlrRdd
      .map(x => (x._1.toString + x._2._3, 1))
      .filter(x => x._1 != null)
      .reduceByKey(_ + _)
    //    (320523197112318119郭静芳,1)
    //    (342726196807071723王磊,1)
    val qlrSum = qlrRdd.keyBy(x => x.x._1.toString + x._2._3).leftOuterJoin(qlrRddCount)
      .map(x => (x._2._1._2._1, x._2._1._2._2, x._2._1._2._3, x._2._2.toList.sorted.last))
      .keyBy(x => x._1.toString)
    //      .take(10).foreach(println)
    //    (bdc-741179,(bdc-741179,320524197911265814,吴冬明,3))
    //    (bdc-739639,(bdc-739639,320524197911265814,吴冬明,3))
    //    (bdc-741208,(bdc-741208,320524197911265814,吴冬明,3))
    //    (bdcdy-69532,(bdcdy-69532,320586198002236817,陈敏,1))
    //    (bdcdy-114502,(bdcdy-114502,110108198002280033,刘一宁,1))

    val baseANDCfAndQlr = baseANDCf
      .map(x => (x._2._6.toString, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6)))
      .leftOuterJoin(qlrSum)
      .filter(x => x._2._2 != None)
      .map(x => (x._2._1._2, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._2.toList.get(0)._4)))

    //    (bdc-795155,(0,bdc-795155,320506432118GB00090F00050206,1.0362386666666668,0.06476491666666667,bdcdy-53768,1))
    //    (bdc-795155,(0,bdc-795155,320506432118GB00090F00050206,1.0362386666666668,0.06476491666666667,bdcdy-53768,1))
    //    (bdc-821399,(0,bdc-821399,320506001057GB00107F00020501,1.3333333333333333,0.13333333333333333,bdcdy-68001,1))


    val qlrRdd2 = qlrRdd.map(x => (x._2._1, x._1 + "^" + x._2._3))
    //    //      println(qlrRdd.count)
    //    //      qlrRdd.take(10).foreach(println)
    //    //    286480
    //    //    (bdc-801674,513023197104056746熊德蓉)
    //    //    (bdc-801973,320524196401137017杭火林)
    //    //    (bdc-802790,320681198910090829陈晓明)
    //
    val fdcqRdd = rowsFdcq.keyBy(x => x(2)).filter(x => x._2(5) != null).filter(x => x._2(5).toString.toInt == 1)
      .map(x => (x._1, x._2(1)))
    //    //      println(fdcqRdd.count)
    //    //      fdcqRdd.take(10).foreach(println)
    //    //    186301
    //    //    (bdcsc-865429,320506104121GB00022F00120906)
    //    //    (bdcsc-863899,320506014003GB00013F00012092)
    //    //    (bdcsc-864088,320506014003GB00013F00011035)
    //
    val qlrAndBdcdy = fdcqRdd.leftOuterJoin(qlrRdd2)
      .filter(x => x._2._2 != None)
      .map(x => (x._2._2.toList.sorted.last, 1))
      .reduceByKey(_ + _)
    //    //    println(qlrAndBdcdy.count)
    //    //    qlrAndBdcdy.take(10).foreach(println)
    //    //    153106
    //    //    (220222197310290034^朴铉哲,1)
    //    //    (342822197103305119^胡少根,2)
    //    //    (370982198207216874^刘传新,1)
    //
    val qlrAndBdcdyAndProid = qlrRdd2.keyBy(x => x._2).leftOuterJoin(qlrAndBdcdy)
      .filter(x => x._2._2 != None)
      .map(x => (x._2._1._1, x._2._2.toList.sorted.last))

    val end = baseANDCfAndQlr.join(qlrAndBdcdyAndProid)
      .map(x => (x._2._1._1, x._2._1._4, x._2._1._5, x._2._1._7, x._2._2))
    //        .take(10).foreach(println)

    //    (bdc-818506,((0,bdc-818506,320506109110GB00170F00722502,0.9534553191489361,0.06810395136778115,bdcdy-69136,1),1097))
    //    (bdc-843175,((0,bdc-843175,320506129022GB00202F00130304,1.1098035842293907,0.05549017921146953,bdcdy-77823,1),425))
    //    (bdc-843175,((0,bdc-843175,320506129022GB00202F00130304,1.1098035842293907,0.05549017921146953,bdcdy-77823,1),425))

    //    val objectEntity = end
    //    .repartition(1).saveAsTextFile("/root/ipf/" + "tt"+nowTime+".csv")
println(end.count)

    //
    val file = "hdfs://master:9000/tt/" + "tt" + nowTime + ".csv"
    val destinationFile = "file:///root/ipf/" + "data_out.csv"
    end.map(x => {
      x._1 + "," + Math.round(x._2.toString.toDouble *100000.0)/100000.0 + "," + Math.round(x._3.toString.toDouble*100000.0)/100000.0 + "," + x._4 + "," + x._5
    })
      .repartition(1).saveAsTextFile(file)

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs._
    val hadoopConfig = new Configuration()
    hadoopConfig.set("mapred.jop.tracker", "hdfs://master:9001")
    hadoopConfig.set("fs.default.name", "hdfs://master:9000")
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(file), new Path(destinationFile).getFileSystem(new Configuration()), new Path(destinationFile), false, hadoopConfig, null)

    println("=============================================================================")

    spark.stop()
  }


}
