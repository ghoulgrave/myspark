import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object MySpark  extends Serializable  {
  def main(args: Array[String]):Unit = {
      val jar: String = "D:\\0-SVNProject\\spark\\spark.jar"
      val jars = ArrayBuffer[String]()
      jars += jar
      val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://master:7077").setJars(jars)
      val spark = new SparkContext(conf)
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
      val count = spark.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / (n - 1))
      spark.stop()
    }

}
