package com.gtmap.spark.etl

import java.io.StringWriter
import java.sql.ResultSet
import java.util.Date

import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVWriter
import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object DataExport {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rtl")
      .setMaster("spark://master:7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)


    fdcq(spark)
//    bdccf(spark)
//    bdcdya(spark)
//    bdcxmrel(spark)
//    qlrxx(spark)
//    xmxx(spark)

    println("OK");
  }

  def bdccf(spark: SparkContext): Unit = {
    val sql :String = " select QLID,BDCDYID,CFFW,CFLX,CFWH,JFSJ,YWH,PROID,QSZT,ISSX from BDC_CF where 1 = ? AND rownum < ?"
    //读取数据
    val bdccf = new JdbcRDD(spark, createConnection, sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues10)
    val objectEntity = bdccf.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
      ,x._2._8,x._2._9,x._2._10).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile("hdfs://master:9000/Bdcdj/" + "cf")
  }

  def bdcdya(spark: SparkContext): Unit = {
    val sql :String = " select qlid,BDCDYID,replace(zwr,chr(13)||chr(10),' ') as zwr,dyfs,ZWLXKSQX,ZWLXJSQX,ZXDYYWH" +
      ",ZXSJ,YWH,PROID,QSZT,case when ZGZQQDSE is not null then ZGZQQDSE else BDBZZQSE * 1.5 end as bdbzzqse,ZGZQQDSE from BDC_DYAQ where 1 = ? AND rownum < ?"
    //读取数据
    val bdcdya = new JdbcRDD(spark, createConnection, sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues13)
        val objectEntity = bdcdya.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
          ,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13).toArray)
          .mapPartitions { data =>
            val stringWriter = new StringWriter();
            val csvWriter = new CSVWriter(stringWriter);
            csvWriter.writeAll(data.toList)
            Iterator(stringWriter.toString)
          }.saveAsTextFile("hdfs://master:9000/Bdcdj/" + "dya")
  }


  def bdcxmrel(spark: SparkContext): Unit = {
    val sql :String = "select  RELID,PROID,QJID,YPROID,YDJXMLY,YQLID from BDC_XM_REL where 1 = ? AND rownum < ?"
    //读取数据
    val bdcxmrel = new JdbcRDD(spark, createConnection, sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues6)
    val objectEntity = bdcxmrel.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile("hdfs://master:9000/Bdcdj/" + "bdcxmrel")
  }

  def qlrxx(spark: SparkContext): Unit = {
    val sql :String = "select qlrid,proid,replace(qlrmc,chr(13)||chr(10),' ') as qlrmc,qlrzjh,qlrlx,replace(qlbl,chr(13)||chr(10),' ') as qlbl, gyfs,qlrxz,sxh,replace(gyqk,chr(13)||chr(10),' ') as gyqk,qlmj,QLRFDDBRZJH " +
      "from bdc_qlr t where  1 = ? AND rownum < ?"
    //读取数据
    val dataXm = new JdbcRDD(spark, createConnection, sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues12)
    //    println(dataXm.keyBy(x => (x._1)).collect().toList)
    val objectEntity = dataXm.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
      ,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile("hdfs://master:9000/Bdcdj/" + "qlrxx")
  }
  def fdcq(spark: SparkContext): Unit = {
    val sql :String = "select qlid,bdcdyid,proid,ghyt,JYJG,qszt from bdc_fdcq t where  1 = ? AND rownum < ?"
    //读取数据
    val dataXm = new JdbcRDD(spark, createConnection, sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues6)
    val objectEntity = dataXm.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile("hdfs://master:9000/Bdcdj/" + "fdcq")
  }




  case class Bdcxm(proid: String, bh: String, qllx: String, djlx: String, sqfbcz: String
                   , xmzt: String,  xmly: String
                   , bjsj: String, lsh: String, sqlx: String, bdcdyid: String
                   , dydjlx: String, bdclx: String,  wiid: String, ybh: String, djzx: String
                   , lzrq: String, zsid: String)
  def xmxx(spark: SparkContext): Unit = {
    val sql1 :String = "select proid, bh, qllx, djlx, sqfbcz, xmzt, xmly, bjsj, lsh, sqlx, bdcdyid, dydjlx, bdclx, wiid" +
      ", ybh, djzx, lzrq, zsid from bdc_xm k where  1 = ? AND rownum < ?"
    //读取数据
    val dataXm = new JdbcRDD(spark, createConnection, sql1, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues18)
//    println(dataXm.keyBy(x => (x._1)).collect().toList)
    val objectEntity = dataXm.keyBy(x => (x._1)).map(x => Bdcxm(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
      ,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13,x._2._14,x._2._15,x._2._16,x._2._17,x._2._18))
    objectEntity.map(bdcxm => List(bdcxm.proid, bdcxm.bh, bdcxm.qllx, bdcxm.djlx, bdcxm.sqfbcz, bdcxm.xmzt, bdcxm.xmly
      , bdcxm.bjsj, bdcxm.lsh, bdcxm.sqlx, bdcxm.bdcdyid, bdcxm.dydjlx, bdcxm.bdclx, bdcxm.wiid
      , bdcxm.ybh, bdcxm.djzx, bdcxm.lzrq, bdcxm.zsid).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile("hdfs://master:9000/Bdcdj/" + "bdcxm")
  }




}
