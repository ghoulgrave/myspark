package com.gtmap.spark.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 从分布式文件系统中将数据拿出来
  */
object DumpDateFromHDFS {

  def getDataFrame(spark: SQLContext, filePath: String): RDD[Row] = {
    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load(filePath)
    df.rdd
  }

}
