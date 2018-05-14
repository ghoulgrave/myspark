package com.gtmap.spark.etl

import java.io.StringWriter
import java.sql.{DriverManager, ResultSet}

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant._
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Exception

object DataCleaning {
  val logger: Logger = LoggerFactory.getLogger(DataCleaning.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rtl")
      .setMaster("spark://master:7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)
    val nowTime = getNowDate
    val filePath = "hdfs://master:9000/BdcdjHRB/"
    val sparkSql = new SQLContext(spark)
    val rowsFdcq: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "fdcq")
    val rowsCf: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "cf")
    val rowsQlr: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "qlrxx")
    val rowsXM: RDD[Row] = DumpDateFromHDFS.getDataFrame(sparkSql, filePath + "bdcxm")

    //    val xmBase = rowsXM
    //      .filter(x => x(0) != null) //proid
    //      .filter(x => x(2) != null) //qllx
    //      .keyBy(x => x(0)) //proid
    //      //proid,qllx
    //      .map(x => (x._1.toString, x._2(2).toString.replace("4,6,8", "4").toInt))
    //      .filter(x => x._2 == 4 || x._2 == 6 || x._2 == 8)
    //
    //    val qlrBase = rowsQlr
    //      .filter(x => x(1) != null) //proid
    //      .filter(x => x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0") // qlrmc 和 qlrzjh
    //      .filter(x => x(4) == "qlr") //qlrlx
    //      .keyBy(x => x(1)) //proid
    //
    //    val end = qlrBase.join(xmBase.keyBy(x => x._1))
    //      .map(x => (x._1, x._2._1))
    //      .join(rowsFdcq.keyBy(x => x(2)))
    //      .map(x => {
    //        if (x._2._1(5) == null) {
    //          (x._2._1(2).toString.replace(",", ""), x._2._1(3).toString, x._2._1(11), x._2._2(1), 1)
    //        } else {
    //          (x._2._1(2).toString.replace(",", ""), x._2._1(3).toString, x._2._1(11), x._2._2(1).toString, x._2._1(5).toString.replace("﹪", "%").toDouble)
    //        }
    //      })
    //      .filter(x => x._3 == null || (x._3.toString != "6" && x._3.toString != "7"))
    //      .map(x => (x._1.toString.replace(",", "，")+ "^" + x._2, x._5))
    //      .map(x => (x._1.toString, x._2.toString.toDouble))
    //      .reduceByKey(_ + _)
    //      .filter(x => x._2 >0)//218396
    //      .map(x=>(x._1,  x._2,x._1.split("\\^")(1)))
    //      .filter(x=> x._3.trim.length == 18)
    //      .map(x=>(x._1,  x._2,x._3.substring(6,10)))
    //      .map(x=>{
    //        var x3Val:Double = 0.0
    //        try{
    //          x3Val= x._3.toDouble
    //        }catch{
    //          case ex:Exception => x3Val= 1970.0
    //        }
    //        if(x3Val <= 0){
    //          x3Val =1970.0
    //        }
    //        (x._1,  x._2*BigInt(10).pow(x3Val.toInt.toString.length -1).toDouble/(x3Val-1900.0))
    //      })
    //      .map(x => (x._1,  Math.sqrt(Math.sqrt(Math.sqrt(x._2)))))
    //      .map(x => (x._1,  Math.sqrt(Math.sqrt(Math.sqrt(x._2*10000)))))
    //      .map(x => (x._1,  Math.sqrt(Math.sqrt(Math.sqrt(Math.sqrt(x._2*10000))))))
    //      .filter(x=>x._2>0)

    val ywr = rowsQlr
      .filter(x => x(1) != null) //proid
      .filter(x => x(3) != null && x(2) != null && x(3) != "0" && x(2) != "0") // qlrmc 和 qlrzjh
      .filter(x => x(4) == "ywr") //qlrlx
      .keyBy(x => x(1))
    val xm1 = rowsXM.filter(x => x(0) != null && x(10) != null).keyBy(x => x(0)+x(10).toString)
      .join(rowsCf.filter(x => x(7) != null && x(1) != null).keyBy(x => x(7)+x(1).toString))
        .map(x=> (x._2._1(0))).keyBy(x=>x)
        .join(ywr) //proid
    xm1.take(10).foreach(println)
    println(xm1.count)// 数据总数有问题
//    7474

    //    val file = "hdfs://master:9000/tt/" + "tt" + nowTime + ".csv"
    //    val destinationFile = "file:///root/ipf/" + "data_out" + nowTime + ".csv"
    //    end.map(x => {
    ////                x._1+","+x._2+","+x._3+","+x._4
    //      x._1 + "," + x._2
    //    }).repartition(1).saveAsTextFile(file)
    //
    //    import org.apache.hadoop.conf.Configuration
    //    import org.apache.hadoop.fs._
    //    val hadoopConfig = new Configuration()
    //    hadoopConfig.set("mapred.jop.tracker", "hdfs://master:9001")
    //    hadoopConfig.set("fs.default.name", "hdfs://master:9000")
    //    val hdfs = FileSystem.get(hadoopConfig)
    //    FileUtil.copyMerge(hdfs, new Path(file), new Path(destinationFile).getFileSystem(new Configuration()), new Path(destinationFile), false, hadoopConfig, null)
    //
    //    println("destinationFile:", destinationFile)

    spark.stop()
  }
}
