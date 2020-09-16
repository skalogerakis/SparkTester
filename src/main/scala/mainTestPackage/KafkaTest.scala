package mainTestPackage

import org.apache.spark.sql.SparkSession
//import spark.implicits._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object KafkaTest {
  //This is how main class is defined in scala
  def main(args: Array[String]): Unit = {
    //This is how structured streaming starts, by building a spark session
    val sparkSession = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()

    import sparkSession.implicits._


    MonitorListener(sparkSession)


    //This hides too much log information and sets log level to error
    sparkSession.sparkContext.setLogLevel("ERROR")

    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("zookeeper.connect","localhost:2181")
      .option("subscribe", "input")

      //      .option("startingOffsets", "earliest")
//      .option("max.poll.records", 10)
//      .option("failOnDataLoss", false)
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]



//    val lines = sparkSession.readStream
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", 9999)
//      .load()
//
//    val words = lines.as[String].flatMap(_.split(" "))
//
//    // Generate running word count
//    val wordCounts = words.groupBy("value").count()
    val wordCount = df.groupBy("key").count()

    // Start running the query that prints the running counts to the console
    val query = wordCount.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "/home/skalogerakis/TUC_Projects/SparkTest/TestCheckpointLocation")
      .format("console")
      .start()

    query.awaitTermination()

  }

  //Function for advanced and more detailed monitoring
  def MonitorListener(sparkSession: SparkSession) : Unit = {
    sparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })
  }

}
