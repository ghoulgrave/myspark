package com.gtmap.spark.etl

import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant.getNowDate
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object OldDate {
  def main(args: Array[String]): Unit = {
    val masterStr = "master";
    val conf = new SparkConf().setAppName("xst")
      .setMaster("spark://" + masterStr + ":7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)
    val nowTime = getNowDate
    val filePath = "hdfs://" + masterStr + ":9000/root/old"

    val sparkSql = new SQLContext(spark)
    val df = sparkSql.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load(filePath)
    //    val df = spark.textFile("file:///root/ipf/xx/ncf_base.csv")
    //        val df = spark.textFile("file:///D:\\root\\ipf\\ncf_base.csv")
    //    df.take(10).foreach(println)
   val oldRdd = df.rdd.keyBy(x => x(0)).map(x=>(x._1.toString.toInt,(x._2(0),x._2(1),x._2(2),x._2(3),x._2(4))))

    val sampleMap = List((1, 1.0), (0, 0.008)).toMap
    val sample2 = oldRdd.sampleByKeyExact(false, sampleMap, 1)

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



  }

}
