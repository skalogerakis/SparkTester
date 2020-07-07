package mainTestPackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object complexWF {
  //This is how main class is defined in scala
  def main(args: Array[String]): Unit = {
    //This is how structured streaming starts, by building a spark session
    val sparkSession = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()

    //This hides too much log information and sets log level to error
    sparkSession.sparkContext.setLogLevel("ERROR")

    MonitorListener(sparkSession)

    //We must add the schema of the file we are going to use
    //We need to explicitly inform spark this is the schema we should be looking for
    val retailDataSchmema = new StructType()
      .add("Id", IntegerType)
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", DateType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    //readStream is a mechanism of reading a particular directory in streaming fashion
    //There is also a header option while reading with true false options
    val streamingData = sparkSession.readStream
      .schema(retailDataSchmema)
      .option("maxFilesPerTrigger","1")  // This option simply makes spark to read up to 2 files per batch
      .csv("/home/skalogerakis/TUC_Projects/SparkTest/MyFiles/TestData")  //Specify directory of file to read

    //Aggregation Example
    val aggrTest = streamingData
      .filter("Quantity > 10")
      .groupBy("InvoiceDate", "Country")
      .agg(avg("UnitPrice"))


    /**
     * Whenever there is checkpoint directory attached to the query, spark goes through the content of the
     * directory before it accepts new data. This makes sure that spark revovers the old state before it starts
     * processing new data. So whenever there is restart, spark first recovers the old state and then start processing new data from the stream.
     */
    val aggrQuery = aggrTest.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "/home/skalogerakis/TUC_Projects/SparkTest/Checkpoint/")
      //.trigger(Trigger.Once)
      .queryName("AggrExample")
      .start()

    println(aggrQuery.lastProgress)


    //Count example
    val countTest = streamingData
      .groupBy("Country")
      .count()

    val countQuery = countTest.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "/home/skalogerakis/TUC_Projects/SparkTest/CheckpointCount/")
      .queryName("CountExample")
      .start()

    println(countQuery.lastProgress)

    aggrQuery.awaitTermination()
    countQuery.awaitTermination()


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
