package com.gtmap.spark.etl

import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant.getNowDate
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

object IAm {
  def main(args: Array[String]): Unit = {
    val masterStr = "master";
    val conf = new SparkConf().setAppName("xst")
      .setMaster("spark://" + masterStr + ":7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)
    val nowTime = getNowDate
    val filePath = "hdfs://" + masterStr + ":9000/BdcdjHRB/"

    val sparkSql = new SQLContext(spark)
    val rowsDY: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "dya")
    val rowsXM: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "bdcxm")
    val rowsFdcq: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "fdcq")
    val rowsCf: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "cf")
    val rowsQlr: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "qlrxx")


    val qlrRdd = rowsQlr
      .filter(x => x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0" && x(1) != null && x(4) != null)
      .keyBy(x => x(1)) // proid match 832364
    //===产权========
    val fdcqRdd = rowsFdcq.filter(x => x(1) != null && x(2) != null)
      .keyBy(x => x(2))
    val qlrRddAndFdcq = qlrRdd.join(fdcqRdd).filter(x => x._2._1(4).toString == "qlr")
      //a.proid,a.proid,a.qlr,a.qlrzjh,a.qlbl,a.gyfs,b.bdcdyid,b.jyjg
      .map(x => (x._1, x._2._1(1), x._2._1(2), x._2._1(3), x._2._1(6), x._2._1(7), x._2._2(1), x._2._2(4)))
      //a.proid,( a.qlr||a.qlrzjh||b.bdcdyid as base ,a.proid,a.qlr,a.qlrzjh,a.qlbl,a.gyfs,b.bdcdyid,b.jyjg)
      .map(x => (x._1, (x._3.toString + x._4.toString + x._7.toString, x._2, x._3, x._4, x._5, x._6, x._7, x._8)))
      .filter(x => x._2._8 != null && x._2._8.toString.toDouble > 0) //proid match 132231

    //======抵押=============
    val dyaRdd = rowsDY.filter(x => x(9) != null).keyBy(x => x(9))
    val qlrRddAndDya = qlrRdd.join(dyaRdd).filter(x => x._2._1(4).toString == "ywr")
      //    b.bdcdyid,b.bdbzzqse,b.zgzqqdse,b.zwlxksqx,b.zwlxjsqx
      .map(x => (x._1, x._2._1(1), x._2._1(2), x._2._1(3), x._2._1(6), x._2._1(7)
      , x._2._2(1), x._2._2(11), x._2._2(12), x._2._2(4), x._2._2(5)))
      .map(x => (x._1, (x._3.toString + x._4.toString + x._7.toString, x._2, x._3, x._4, x._5, x._6
        , x._7, x._8, x._9, x._10, x._11))) //proid mach 99159

    //=====查封======
    val cfRdd = rowsCf.filter(x => x(7) != null).keyBy(x => x(7))
    val qlrRddAndCf = qlrRdd.join(cfRdd).filter(x => x._2._1(4).toString == "ywr")
      //a.proid,a.proid,a.qlr,a.qlrzjh,a.qlbl,a.gyfs
      .map(x => (x._1, x._2._1(1), x._2._1(2), x._2._1(3), x._2._1(6), x._2._1(7), x._2._2(1), 1))
      .filter(x => x._7 != null)
      .map(x => (x._1, (x._3.toString + x._4.toString + x._7.toString, x._2, x._3, x._4, x._5, x._6
        , x._7, x._8))) //proid mach 6129
    val qlrFcdyaCfBase = qlrRddAndFdcq.keyBy(x => x._2._1).leftOuterJoin(qlrRddAndDya.keyBy(x => x._2._1))
      .leftOuterJoin(qlrRddAndCf.keyBy(x => x._2._1)) // match 132444


    //=============
    val baseChange = qlrFcdyaCfBase.map(x => {
      if (x._2._2 == None) {
        (x._1, x._2._1, 0)
      } else {
        (x._1, x._2._1, 1)
      }
    })
      .filter(x => x._2._2 != None)
      .distinct()
      .map(x => {
        //cf,base,qlbl,gyfs,jyjg
        (x._3, x._1, x._2._1._2._5, x._2._1._2._6, x._2._1._2._8, x._2._2.toList(0), x._2._1._2._3, x._2._1._2._4)
      })
      .map(x => (x._1, x._2, x._3, x._4, x._5, x._6._2._8, x._6._2._10, x._6._2._11, x._7, x._8))
      .filter(x => x._8 != null && x._6 != null && x._7 != null)
      .filter(x => x._6.toString.toDouble > 0)

    val qlrBdcdyCount = baseChange.map(x => {
      if (x._4.toString == "0") {
        (x._9.toString + x._10.toString, 1.0)
      } else if (x._4.toString == "1") {
        (x._9.toString + x._10.toString, 0.5)
      } else {
        (x._9.toString + x._10.toString, x._3.toString.toDouble)
      }
    })
      .reduceByKey(_ + _)

    val qlrdycffc = baseChange.keyBy(x => x._9.toString + x._10.toString)
      .join(qlrBdcdyCount)
      .map(x => (x._1, x._2._1, x._2._2))
      //xm+zjh,cf,jyjg,bdbzzq,贷款年限,不动产数量
      .map(x => (x._1, x._2._1, x._2._5, x._2._6, Math.round((new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._8.toString).getTime -
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(x._2._7.toString).getTime) / (1000 * 60 * 60 * 24 * 365.0)), x._3))

    val dyff = qlrdycffc.map(x => (x._1, 1))
      .reduceByKey(_ + _)

    val end = qlrdycffc.keyBy(x => x._1).join(dyff)
      .distinct()
      //cf,jyjg,bdbzzq,贷款年限,不动产数量
      .map(x => (x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._2))
      .map(x => (x._1, x._2.toString.toDouble / x._3.toString.toDouble, x._3.toString.toDouble / x._4.toString.toDouble, x._5, x._6))

    val sampleMap = List((1, 1.0), (0, 0.01)).toMap
    val sample2 = end.keyBy(x => x._1).sampleByKeyExact(false, sampleMap, 1)

    val file = "hdfs://" + masterStr + ":9000/tt/" + "tt" + nowTime + ".csv"
    val destinationFile = "file:///root/ipf/" + "data_out" + nowTime + ".csv"
    sample2.map(x => {
      //        x._1 + "," + x._2 + "," + x._3 + "," + x._4
      x._2._1 + "," + x._2._2 + "," + x._2._3 + "," + x._2._4 + "," + x._2._5
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
    //      .collect
    //    sample2.take(100).foreach(println)
    //    println( sample2.count)
    //            .take(100).foreach(println)

    println("=============================================================================")
    spark.stop()
  }

}
