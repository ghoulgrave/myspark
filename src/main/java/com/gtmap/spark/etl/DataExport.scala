package com.gtmap.spark.etl

import java.io.StringWriter
import java.sql.{Connection, ResultSet}
import java.util.Date

import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVWriter
import com.gtmap.spark.common.Constant
import com.gtmap.spark.common.Constant._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object DataExport {
  //连接
  val conn: String = "jdbc:oracle:thin:@192.168.10.200:1521:orcl"
  //用户
  val user: String = "BDCDJ_ZHJG" //连接数据库用户名
  //密码
  val pwd: String = "gtis" //连接数据库密码

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("export")
      .setMaster("spark://master:7077").setJars(Constant.jars)
    val spark = new SparkContext(conf)

    //文件上传位置
    val filePath: String = "hdfs://master:9000/BdcdjHRB/"

//    fdcq(spark,filePath)
//    bdccf(spark,filePath)
//    bdcdya(spark,filePath)
//    bdcxmrel(spark,filePath)
//    qlrxx(spark,filePath)
//    xmxx(spark,filePath)

    println("OK");
  }

  def bdccf(spark: SparkContext,filePath:String): Unit = {
//    val connn = createConnection//定义变量会出现序列化问题,暂时没有解决
    val sql :String = " select QLID,BDCDYID,CFFW,CFLX,CFWH,JFSJ,YWH,PROID,QSZT,0 as ISSX from BDC_CF where 1 = ? AND rownum < ?"
    //读取数据
    val bdccf = new JdbcRDD(spark, ()=> createConnection(conn,user,pwd), sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues10)
    val objectEntity = bdccf.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
      ,x._2._8,x._2._9,x._2._10).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile(filePath + "cf")
  }

  def bdcdya(spark: SparkContext,filePath:String): Unit = {
    //0,1,2,3,4,5,6
    val sql :String = " select qlid,BDCDYID,replace(zwr,chr(13)||chr(10),' ') as zwr,dyfs,ZWLXKSQX,ZWLXJSQX,ZXDYYWH" +
     //,7,8,9,10,11,12
      ",ZXSJ,YWH,PROID,QSZT,bdbzzqse,ZGZQQDSE from BDC_DYAQ where 1 = ? AND rownum < ?"
    //读取数据
    val bdcdya = new JdbcRDD(spark, ()=> createConnection(conn,user,pwd), sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues13)
        val objectEntity = bdcdya.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
          ,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13).toArray)
          .mapPartitions { data =>
            val stringWriter = new StringWriter();
            val csvWriter = new CSVWriter(stringWriter);
            csvWriter.writeAll(data.toList)
            Iterator(stringWriter.toString)
          }.saveAsTextFile(filePath + "dya")
  }

  def bdcxmrel(spark: SparkContext,filePath:String): Unit = {
    val sql :String = "select  RELID,PROID,QJID,YPROID,YDJXMLY,'' as YQLID from BDC_XM_REL where 1 = ? AND rownum < ?"
    //读取数据
    val bdcxmrel = new JdbcRDD(spark, ()=> createConnection(conn,user,pwd), sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues6)
    val objectEntity = bdcxmrel.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile(filePath + "bdcxmrel")
  }

  def qlrxx(spark: SparkContext,filePath:String): Unit = {
    val sql :String = "select qlrid,proid,replace(qlr,chr(13)||chr(10),' ') as qlr,qlrzjh,qlrlx,replace(qlbl,chr(13)||chr(10),' ') as qlbl, gyfs,qlrxz,sxh,'' as gyqk,0.0 as qlmj,qlrsfzjzl " +
      "from bdc_qlr t where  1 = ? AND rownum < ?"
    //读取数据
    val dataXm = new JdbcRDD(spark, ()=> createConnection(conn,user,pwd), sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues12)
    //    println(dataXm.keyBy(x => (x._1)).collect().toList)
    val objectEntity = dataXm.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
      ,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile(filePath + "qlrxx")
  }
  def fdcq(spark: SparkContext,filePath:String): Unit = {
    val sql :String = "select qlid,bdcdyid,proid,'' as ghyt,JYJG,qszt from bdc_fdcq t where  1 = ? AND rownum < ?"
    //读取数据
    val dataXm = new JdbcRDD(spark, ()=> createConnection(conn,user,pwd), sql, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues6)
    val objectEntity = dataXm.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile(filePath + "fdcq")
  }
  def xmxx(spark: SparkContext,filePath:String): Unit = {
    val sql1 :String = "select proid, bh, qllx, djlx, sqfbcz, xmzt, xmly, bjsj, lsh, sqlx, bdcdyid, dydjlx, bdclx, wiid" +
      ", '' as ybh, '' as djzx, '' as lzrq, '' as zsid from bdc_xm k where  1 = ? AND rownum < ?"
    //读取数据
    val dataXm = new JdbcRDD(spark, ()=> createConnection(conn,user,pwd), sql1, lowerBound = 1, upperBound = 999999, numPartitions = 1
      , mapRow = extractValues18)
//    println(dataXm.keyBy(x => (x._1)).collect().toList)
    val objectEntity = dataXm.keyBy(x => (x._1)).map(x => List(x._1, x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7
      ,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13,x._2._14,x._2._15,x._2._16,x._2._17,x._2._18).toArray)
//    objectEntity.map(bdcxm => List(bdcxm.proid, bdcxm.bh, bdcxm.qllx, bdcxm.djlx, bdcxm.sqfbcz, bdcxm.xmzt, bdcxm.xmly
//      , bdcxm.bjsj, bdcxm.lsh, bdcxm.sqlx, bdcxm.bdcdyid, bdcxm.dydjlx, bdcxm.bdclx, bdcxm.wiid
//      , bdcxm.ybh, bdcxm.djzx, bdcxm.lzrq, bdcxm.zsid).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile(filePath + "bdcxm")
  }




}
