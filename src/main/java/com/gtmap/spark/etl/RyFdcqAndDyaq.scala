package com.gtmap.spark.etl

import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant.getNowDate
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

object RyFdcqAndDyaq {

  def main(args: Array[String]): Unit = {
    val masterStr = "172.16.175.128";
    val conf = new SparkConf().setAppName("rtl")
      .setMaster("spark://" + masterStr + ":7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)
    val nowTime = getNowDate
    val filePath = "hdfs://" + masterStr + ":9000/BdcdjHRB/"
    val sparkSql = new SQLContext(spark)
    val rowsFdcq: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "fdcq")
    val rowsCf: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "cf")
    val rowsDY: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "dya")
    val rowsQlr: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "qlrxx")
    val rowsXM: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "bdcxm")

    //权利人信息,proid为主键
    val qlrByproid = rowsQlr
      .filter(x => x(1) != null) //proid
      .filter(x => x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0") // qlrmc 和 qlrzjh
      .keyBy(x => x(1)) //proid

    //    qlrByproid.filter(x => x._2(3).toString.contains("230104197906122617"))
    //      .take(10).foreach(println)
    //    println("=qlrAll=")


    val ywr = qlrByproid.filter(x => x._2(4) == "ywr")

    //    ywr.filter(x => x._2(3).toString.contains("230104197906122617"))
    //      .take(10).foreach(println)
    //    println("=ywr=")


    //权利人抵押数量
    val qlrDySl = rowsDY
      .filter(x => x(9) != null).keyBy(x => (x(9)))
      .join(ywr)
      .filter(x => x._2._1(10) != null && x._2._1(10).toString.toInt == 1)
      .map(x => (x._2._2(2).toString.replace(",", "，") + "^" + x._2._2(3).toString().trim, 1))
      .reduceByKey(_ + _)


    //    qlrDySl.filter(x => x._1.toString.contains("230104197906122617"))
    //      .take(10).foreach(println)
    //    println("=dy=")

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

    //    qlrCq.filter(x => x._1.toString.contains("230104197906122617"))
//          .take(10).foreach(println)
    //    println("=cq=")


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
//
//    //    qlrCf.filter(x => x._1.toString.contains("230104197906122617"))
//    //      .take(10).foreach(println)
//    //    println("=cf=")
//
//
    val endRdd = qlrCq.leftOuterJoin(qlrCf)
      .map(x => {
        if (x._2._2 == None) {
          (x._1, x._2._1, 0)
        } else {
          (x._1, x._2._1, x._2._2.toList.sorted.last)
        }
      })
//  .keyBy(x => x._1)
//      .leftOuterJoin(qlrDySl)
//      .map(x => {
//        //名+证件号,是否查封,产权数量,抵押数量
//        if (x._2._2 == None) {
//          (x._2._1._1, x._2._1._3, x._2._1._2, 0)
//        } else {
//          (x._2._1._1, x._2._1._3, x._2._1._2, x._2._2.toList.sorted.last)
//        }
//      })
//      .filter(x => x._4 >0)
////      .take(10).foreach(println)
//
//
    val file = "hdfs://" + masterStr + ":9000/tt/" + "tt" + nowTime + ".csv"
    val destinationFile = "file:///root/ipf/" + "data_out" + nowTime + ".csv"


    endRdd.map(x => {
//        x._1 + "," + x._2 + "," + x._3 + "," + x._4
                  x._1 + "," + x._2 + "," + x._3
      })
      .repartition(1)
      .distinct.saveAsTextFile(file)

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs._
    val hadoopConfig = new Configuration()
    hadoopConfig.set("mapred.jop.tracker", "hdfs://" + masterStr + ":9001")
    hadoopConfig.set("fs.default.name", "hdfs://" + masterStr + ":9000")
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(file), new Path(destinationFile).getFileSystem(new Configuration()), new Path(destinationFile), false, hadoopConfig, null)

    println("destinationFile:", destinationFile)

    spark.stop()
  }
}
