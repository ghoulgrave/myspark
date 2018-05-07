package com.gtmap.spark.common

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ArrayBuffer

object Constant {
  //驱动
  val driver: String = "oracle.jdbc.driver.OracleDriver"
  //连接
  val conn: String = "jdbc:oracle:thin:@192.168.10.200:1521:orcl"
  //用户
  val user: String = "BDCDJ_WZ" //连接数据库用户名
  //密码
  val pwd: String = "gtis" //连接数据库密码

  //jar包位置
  val jar: String =  System.getProperty("user.dir")+"\\spark.jar"
  val jdbc: String = "D:\\spark-2.1.1-bin-hadoop2.7\\ojdbc-14.jar"
  val jars = ArrayBuffer[String](jar, jdbc)

  //连接ORACLE数据库函数
  def createConnection() = {
    Class.forName(driver).newInstance()
    DriverManager.getConnection(conn,user,pwd)
  }
  def getNowDate():String={
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val sdf = dateFormat.format( now )
    sdf
  }
  //处理结果集函数
  def extractValues2(r: ResultSet) = {
    (r.getString(1), r.getString(2))
  }
  //处理结果集函数
  def extractValues3(r: ResultSet) = {
    (r.getString(1), r.getString(2),r.getString(3))
  }
  //处理结果集函数
  def extractValues5(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3)
      , r.getString(4), r.getString(5))
  }
  //处理结果集函数
  def extractValues6(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3)
      , r.getString(4), r.getString(5), r.getString(6))
  }
  //处理结果集函数
  def extractValues12(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3)
      , r.getString(4), r.getString(5), r.getString(6)
      , r.getString(7), r.getString(8), r.getString(9)
      , r.getString(10),r.getString(11), r.getString(12))
  }
  //处理结果集函数
  def extractValues10(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3)
      , r.getString(4), r.getString(5), r.getString(6)
      , r.getString(7), r.getString(8), r.getString(9)
      , r.getString(10))
  }
  //处理结果集函数
  def extractValues13(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3)
      , r.getString(4), r.getString(5), r.getString(6)
      , r.getString(7), r.getString(8), r.getString(9)
      , r.getString(10),r.getString(11), r.getString(12)
      , r.getString(13))
  }
  //处理结果集函数
  def extractValues18(r: ResultSet) = {
    (r.getString(1), r.getString(2), r.getString(3)
      , r.getString(4), r.getString(5), r.getString(6)
      , r.getString(7), r.getString(8), r.getString(9)
      , r.getString(10),r.getString(11), r.getString(12)
      , r.getString(13), r.getString(14), r.getString(15)
      , r.getString(16), r.getString(17), r.getString(18))
  }

  case class Bdcdy(proid: String, bh: String,bdcdyid: String,zwr: String)
}
