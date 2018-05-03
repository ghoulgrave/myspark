import java.io.StringWriter
import java.sql.{DriverManager, ResultSet}

import JDBC.Bdcdy
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.StringType
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object JDBC extends Serializable  {
  val driver_str = "oracle.jdbc.driver.OracleDriver"
  val conn_str = "jdbc:oracle:thin:@192.168.10.200:1521:orcl"
  val user_str = "BDCDJ_WZ" //连接数据库用户名
  val pwd_str = "gtis"  //连接数据库密码

  val jar: String = "D:\\0-SVNProject\\spark\\spark.jar"
  val jdbc= "D:\\spark-2.1.1-bin-hadoop2.7\\ojdbc-14.jar"
  val jars = ArrayBuffer[String]()
  jars += jar
  jars += jdbc

  //连接ORACLE数据库函数
  def createConnection() = {
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    DriverManager.getConnection(conn_str,user_str,pwd_str)
  }
  //处理结果集函数
  def extractValues2(r: ResultSet) = {
    (r.getString(1), r.getString(2))
  }
  //处理结果集函数
  def extractValues3(r: ResultSet) = {
    (r.getString(1), r.getString(2),r.getString(3))
  }

  case class Bdcdy(proid: String, bh: String,bdcdyid: String,zwr: String)
  def main(args: Array[String]):Unit = {

    val conf = new SparkConf().setAppName("xx")
      .setMaster("spark://master:7077").setJars(jars)
    val spark = new SparkContext(conf)

    //读取数据
//    val dataXm = new JdbcRDD(spark,createConnection,"select proid,BH from BDC_XM WHERE 1 = ? AND rownum < ?"
//      , lowerBound = 1, upperBound = 99999, numPartitions = 1,mapRow = extractValues2 )
//    val dataDy = new JdbcRDD(spark,createConnection,"select PROID,bdcdyid,zwr from BDC_DYAQ WHERE 1 = ? AND rownum < ?"
//      , lowerBound = 1, upperBound = 99999, numPartitions = 1,mapRow = extractValues3 )
//  //可以将rdd连接后生成新的rdd格式
//    val result: RDD[Array[String]] =
//    dataXm.keyBy(x => (x._1)).join(dataDy.keyBy(y => y._1)).map {x  =>  Array(x._1,x._2._1._2,x._2._2._2,x._2._2._3) }

//    //以csv格式输出
//    val bdcdyEntity = dataXm.keyBy(x => (x._1)).join(dataDy.keyBy(y => y._1)).map(x => Bdcdy(x._1,x._2._1._2,x._2._2._2,x._2._2._3))
//    bdcdyEntity.map(bdcdy => List(bdcdy.proid,bdcdy.bh,bdcdy.bdcdyid,bdcdy.zwr).toArray)
//      .mapPartitions { data =>
//        val stringWriter = new StringWriter();
//        val csvWriter = new CSVWriter(stringWriter);
//        csvWriter.writeAll(data.toList)
//        Iterator(stringWriter.toString)
//      }.saveAsTextFile("hdfs://master:9000/user/ytr99")


//    //读取csv格式文件到dataframe中
//    val spark1 = new SQLContext(spark)
//    val df = spark1.read.format("com.databricks.spark.csv")
//      .option("header", "false")
//      .option("inferSchema", "false") //是否自动推到内容的类型
//      .option("delimiter",",")  //分隔符，默认为 ,
//      .load("hdfs://master:9000/user/ytr99")
//    //展示dataframe
//    df.show()
//    //转化为rdd
//    val rows: RDD[Row] = df.rdd
//    val xx = rows.keyBy(x=>x(0)).map(x => (x._1.toString,(x._2(0),x._2(1),x._2(2))))
//    //rdd之间关联生成新的rdd
//    val mm = dataXm.keyBy(x => (x._1)).join(xx).map(x => (x._1,x._2._1._1,x._2._2))
//    print(mm.collect().toList)

//=============================================================================
//    //建立一个基本的键值对RDD，包含ID和名称，其中ID为1、2、3、4cd
//    val rdd1 = spark.makeRDD(Array(("1","Spark"),("2","Hadoop"),("3","Scala"),("4","Java")),2)
//    //建立一个行业薪水的键值对RDD，包含ID和薪水，其中ID为1、2、3、5
//    val rdd2 = spark.makeRDD(Array(("1","30K"),("2","15K"),("3","25K"),("5","10K")),2)

//    建议使用 take(): rdd.take(100).foreach(println)，
//    而不使用rdd.collect().foreach(println)。

    spark.stop()
  }
}
