package mainTestPackage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

//https://www.atozlearner.com/distributed-computing/2018/12/13/setup-apache-spark-intellij/

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger;
    rootLogger.setLevel(Level.ERROR)

    val input = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

    input.foreach(println)

  }
}
