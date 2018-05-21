package com.gtmap.spark.etl


import java.text.SimpleDateFormat
import java.util.Date

import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant._
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._


object DataFromFile {

  def main(args: Array[String]): Unit = {

    val masterStr = "172.16.175.128";
    val conf = new SparkConf().setAppName("xst")
      .setMaster("spark://" + masterStr + ":7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)
    val nowTime = getNowDate
    val filePath = "hdfs://"+masterStr+":9000/BdcdjHRB/"

    val sparkSql = new SQLContext(spark)
    val rowsDY: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql,filePath + "dya")
    val rowsXM: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql,filePath + "bdcxm")
    val rowsFdcq: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql,filePath + "fdcq")
    val rowsCf: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql,filePath + "cf")
    val rowsQlr: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql,filePath + "qlrxx")

    val dyRdd = rowsDY
      .filter(x => x(9) != null)
      .keyBy(x => x(9))
      .map(x => Bdcdya(x._2(0), x._2(1), x._2(2), x._2(3), x._2(4)
        , x._2(5), x._2(6), x._2(7), x._2(8), x._2(9).toString
        , x._2(10), x._2(11), x._2(12)))
      .map(x => (x.dyproid, x))

    val xmRdd = rowsXM
      .filter(x => x(0) != null)
      .keyBy(x => x(0))
      .map(x => Bdcxm(x._1.toString, x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)
        , x._2(9), x._2(10), x._2(11), x._2(12), x._2(13), x._2(14), x._2(15), x._2(16), x._2(17)))
      .filter(x => x.bdcdyid != null)
      .filter(x=>x.xmzt!= null && x.xmzt.toString.toInt == 1)
      .map(x => (x.proid, x))

    //proid关联抵押登记结束时间
    val dysjRdd = dyRdd.leftOuterJoin(xmRdd)
      .filter(x => x._2._2 != None)
      .filter(x => x._2._1.bdbzzqse != null && x._2._1.bdbzzqse.toString.toDouble > 0)
      .map(x => (x._2._1.dyproid, x._2._1.dybdcdyid, x._2._1.zwlxksqx, x._2._1.zwlxjsqx, x._2._1.bdbzzqse, x._2._2.get.bjsj))
      .filter(x => x._6 != null)
    //    (bdcdy-76229,320506102085GB00194F00012308,2016-11-11 00:00:00.0,2026-11-11 00:00:00.0,21,2016-12-10 11:13:08.0)
    //    (bdcdy-58177,320506001081GB00026F00211104,2014-08-28 00:00:00.0,2044-10-28 00:00:00.0,79,2016-09-08 09:17:13.0)

    //bdcdyid分组
    val xmRddByBdcdyid = rowsXM.filter(x => x(10) != null).keyBy(x => x(10)).map(x => (x._2(10), (x._2(0), x._2(1), x._2(2), x._2(3)
      , x._2(4), x._2(5), x._2(6), x._2(7), x._2(8), x._2(9), x._2(10), x._2(11), x._2(12), x._2(13)
      , x._2(14), x._2(15), x._2(16), x._2(17))))
    val xmWithSj = xmRddByBdcdyid.join(dysjRdd.keyBy(x => x._2)).filter(x => x._2._2 != None)
      .filter(x => x._2._1._8 != null && x._2._2._6 != null)
      .filter(x => x._2._1._3 != null && (x._2._1._3.toString.toInt == 4 || x._2._1._3.toString.toInt == 6 || x._2._1._3.toString.toInt == 8))
      .filter(x => x._2._2._3 != null && x._2._2._4 != null)
    val totAllSj = xmWithSj.map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._8, x._2._1._11
      //nd.qlid, nd.BDCDYID, nd.ZWLXKSQX, nd.ZWLXJSQX, nd.BDBZZQSE, nd.BJSJ
      , x._2._2._1, x._2._2._2, x._2._2._3, x._2._2._4, x._2._2._5, x._2._2._6, Math.abs(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._1._8.toString).getTime -
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._2._6.toString).getTime), x._2._2._1
    )))
    //    (320506001117GB00227F02690702,(bdc-772198,201606006040617,4,2016-09-14 10:07:16.0,320506001117GB00227F02690702,bdcdy-57595,320506001117GB00227F02690702,2014-11-30 00:00:00.0,2044-11-30 00:00:00.0,60,2016-09-06 12:03:47.0,684209000,bdcdy-57595))
    //    (320506109097GB00005F00101804,(bdc-849299,201606006101139,4,2017-01-17 13:12:15.0,320506109097GB00005F00101804,bdcdy-80785,320506109097GB00005F00101804,2015-05-18 00:00:00.0,2035-05-17 00:00:00.0,68,2016-12-28 09:56:02.0,1739773000,bdcdy-80785))

    //排序并获取最小时间
    val xmMinSj = xmWithSj.map(x => (x._2._1._1,
      Math.abs((new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._1._8.toString).getTime -
        new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._2._6.toString).getTime))
    )).sortBy(x => x._1.toString).groupByKey().map(x => (x._1, x._2.toList.sorted.reverse.last))
    //    (bdcdy-44722,0)
    //    (bdcdy-53768,0)

    //项目和抵押信息
    val xmAndDyxx = totAllSj.map(x => (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7
      , x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13))
      .keyBy(x => x._1).leftOuterJoin(xmMinSj)
      .map(x => (x._2._1._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10, x._2._1._11
        , x._2._1._12, x._2._2.toList.sorted.reverse.last, x._2._1._13)))
      .filter(x => x._2._12 == x._2._13).filter(x => x._2._9 != null)
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

    val cfByBdcdy = rowsCf.keyBy(x => x(1)).map(x => (x._1, 1))

    val xmByBdcdy = rowsXM.filter(x => x(10) != null).keyBy(x => x(10)).map(x => (x._1, (x._2(0))))
    //      .take(10).foreach(println)
    val bdccf = xmByBdcdy.leftOuterJoin(cfByBdcdy).map(x => {
      if (x._2._2 == None) {
        (x._2._1, 0)
      } else {
        (x._2._1, 1)
      }
    }).distinct()

    val baseANDCf = base.leftOuterJoin(bdccf)
      //(proid,(是否有查封,proid,交易价格是否超过评估价格,年还款比例))
      .map(x => (x._1, x._2._2.toList.sorted.last, x._2._1._1, x._2._1._5, x._2._1._2, x._2._1._3, x._2._1._4.toString))
    //      .take(10).foreach(println)
    //    (bdc-918950,(0,bdc-918950,320506109110GB00175F00691703,1.4096942857142858,0.050346224489795914,bdcdy-112453))
    //    (bdc-830728,(0,bdc-830728,320506109091GB00068F00020801,0.9528985507246377,0.03176328502415459,bdcdy-71941))
    //    (bdc-814539,(0,bdc-814539,320506109033GB00015F00100301,0.9527363184079602,0.03175787728026534,bdcdy-69312))

    val end = this.bdcQlr(rowsQlr,rowsDY,rowsXM,rowsFdcq,rowsCf)

    //权利人信息,proid为主键
    val qlrByproid = rowsQlr
      .filter(x => x(1) != null) //proid
      .filter(x => x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0") // qlrmc 和 qlrzjh
      .keyBy(x => x(1)) //proid

    val qlrAllByxmzjh = qlrByproid.map(x => (x._1.toString,x._2(2).toString.replace(",", "，").trim,x._2(3).toString.trim))
      .map(x=>(x._2+"^"+x._3,x._1))
      .keyBy(x=>x._1)

    val qlrByPro = qlrAllByxmzjh.leftOuterJoin(end.keyBy(x=>x._1))
//      .filter(x=>x._2._2 != None)
//      .take(10).foreach(println)

      baseANDCf.keyBy(x=>x._1).join(qlrByPro.keyBy(x=>x._2._1._2))
      .take(10).foreach(println)

//    end.take(10).foreach(println)

//    val file = "hdfs://" + masterStr + ":9000/tt/" + "tt" + nowTime + ".csv"
//    val destinationFile = "file:///root/ipf/" + "data_out" + nowTime + ".csv"
//    endRdd.map(x => {
//      //        x._1 + "," + x._2 + "," + x._3 + "," + x._4
//      x._1 + "," + x._2 + "," + x._3
//    })
//      .repartition(1)
//      .distinct.saveAsTextFile(file)
//
//    import org.apache.hadoop.conf.Configuration
//    import org.apache.hadoop.fs._
//    val hadoopConfig = new Configuration()
//    hadoopConfig.set("mapred.jop.tracker", "hdfs://" + masterStr + ":9001")
//    hadoopConfig.set("fs.default.name", "hdfs://" + masterStr + ":9000")
//    val hdfs = FileSystem.get(hadoopConfig)
//    FileUtil.copyMerge(hdfs, new Path(file), new Path(destinationFile).getFileSystem(new Configuration()), new Path(destinationFile), false, hadoopConfig, null)
//    println("destinationFile:", destinationFile)

    println("=============================================================================")
    spark.stop()
  }



  def bdcQlr(rowsQlr: RDD[Row],rowsDY: RDD[Row],rowsXM: RDD[Row],rowsFdcq: RDD[Row],rowsCf: RDD[Row]) = {

    //权利人信息,proid为主键
    val qlrByproid = rowsQlr
      .filter(x => x(1) != null) //proid
      .filter(x => x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0") // qlrmc 和 qlrzjh
      .keyBy(x => x(1)) //proid
    val ywr = qlrByproid.filter(x => x._2(4) == "ywr")

    //权利人抵押数量
    val qlrDySl = rowsDY
      .filter(x => x(9) != null).keyBy(x => (x(9)))
      .join(ywr)
      .filter(x => x._2._1(10) != null && x._2._1(10).toString.toInt == 1)
      .map(x => (x._2._2(2).toString.replace(",", "，").trim + "^" + x._2._2(3).toString().trim, 1))
      .reduceByKey(_ + _)

    //基础项目表
    val xmBase = rowsXM
      .filter(x => x(0) != null) //proid
      .filter(x => x(2) != null) //qllx
      .keyBy(x => x(0)) //proid
      //proid,qllx
      .map(x => (x._1.toString, x._2(2).toString.replace("4,6,8", "4").toInt))
      .filter(x => x._2 == 4 || x._2 == 6 || x._2 == 8)

    val qlrCq = qlrByproid
      .filter(x => x._2(4) == "qlr")
      .join(xmBase.keyBy(x => x._1))
      .map(x => (x._1, x._2._1))
      .join(rowsFdcq.keyBy(x => x(2)))
      .map(x => {
        if (x._2._1(5) == null) {
          (x._2._1(2).toString.replace(",", ""), x._2._1(3).toString, x._2._1(11), x._2._2(1), 1)
        } else {
          (x._2._1(2).toString.replace(",", ""), x._2._1(3).toString, x._2._1(11), x._2._2(1).toString, x._2._1(5).toString.replace("﹪", "%").toDouble)
        }
      })
      .filter(x => x._3 == null || (x._3.toString != "6" && x._3.toString != "7"))
      .map(x => (x._1.toString.replace(",", "，") + "^" + x._2, x._5))
      .map(x => (x._1.toString, x._2.toString.toDouble))
      .reduceByKey(_ + _)
      .filter(x => x._2 > 0) //218396
      .map(x => (x._1,x._2, x._1.split("\\^")(1)))
      .filter(x => x._3.trim.length == 18)
      .map(x => (x._1, x._2, x._3.substring(6, 12)))
      .map(x => {
        var x3Val: Double = 0.0
        try {
          x3Val = x._3.toDouble
        } catch {
          case ex: Exception => x3Val = 197001.0
        }
        if (x3Val <= 0) {
          x3Val = 197001.0
        }
        (x._1, x._2 * BigInt(10).pow(x3Val.toInt.toString.length - 1).toDouble / (x3Val - 190001.0) )
      })
      .map(x => (x._1, Math.sqrt(Math.sqrt(Math.sqrt(x._2)))))
      //      .map(x => (x._1, Math.sqrt(Math.sqrt(Math.sqrt(x._2 * 10000)))))
      //      .map(x => (x._1, Math.sqrt(Math.sqrt(Math.sqrt(Math.sqrt(x._2 * 10000))))))
      .filter(x => x._2 > 0)


    //查封项目
    val xmCf = rowsXM.filter(x => x(0) != null && x(10) != null).keyBy(x => x(0) + x(10).toString)
      .join(rowsCf.filter(x => x(7) != null && x(1) != null).keyBy(x => x(7) + x(1).toString))
      .map(x => (x._2._1(0))).keyBy(x => x)

    val qlrCf = ywr.leftOuterJoin(xmCf) //proid
      .map(x => {
      if (x._2._2 == None) {
        (x._2._1(0), x._2._1(1), x._2._1(2), x._2._1(3), x._2._1(4), 0)
      } else {
        (x._2._1(0), x._2._1(1), x._2._1(2), x._2._1(3), x._2._1(4), 1)
      }
    })
      .map(x => (x._3.toString.replace(",", "，") + "^" + x._4.toString.trim, x._6))
      .reduceByKey(_ + _)
      .map(x => {
        if (x._2 > 0) {
          (x._1, 1)
        } else {
          (x._1, 0)
        }
      })
    val endRdd = qlrCq.leftOuterJoin(qlrCf)
      .map(x => {
        if (x._2._2 == None) {
          (x._1, x._2._1, 0)
        } else {
          (x._1, x._2._1, x._2._2.toList.sorted.last)
        }
      })
    endRdd
  }

}
