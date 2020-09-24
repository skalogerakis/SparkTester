package mainTestPackage

import net.heartsavior.spark.sql.state.StateInformationInCheckpoint
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object WatermarkExample {
  //This is how main class is defined in scala
  def main(args: Array[String]): Unit = {
    //This is how structured streaming starts, by building a spark session
    val sparkSession = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()

    //    val stateInfo = new StateInformationInCheckpoint(sparkSession).gatherInformation(new Path("/home/skalogerakis/TUC_Projects/SparkTest/Checkpoint/"))

    MonitorListener(sparkSession)


    //This hides too much log information and sets log level to error
    sparkSession.sparkContext.setLogLevel("ERROR")

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


    val timeKey = "processing_time"
    val watermarkThreshold = "0 seconds"
    val windowThreshold = "10 seconds"

    //Added watermarks to look more alike what I am trying to achieve. Here I add a column with timestamp
    val my_stream = streamingData
//      .map(getRowMapper(key, valueKey), Encoders.tuple(Encoders.STRING, Encoders.DOUBLE))
//      .toDF(value1, value2)
      .withColumn(timeKey, functions.current_timestamp)
      .withWatermark(timeKey, watermarkThreshold)
      .groupBy(functions.window(functions.col(timeKey), windowThreshold), functions.col("InvoiceDate"))
      .agg(avg("UnitPrice"))

    //Parse output to a json file. TODO see if overhead with hdfs creates issues
    val aggrQuery :StreamingQuery = my_stream.writeStream
      .format("json")
      .outputMode("append")
      .option("path", "/home/skalogerakis/TUC_Projects/SparkTest/WatermarkExampleResult/")
      .option("checkpointLocation", "/home/skalogerakis/TUC_Projects/SparkTest/WatermarkExample/")
      .queryName("WatermarkExample")
      .start()

    //    println(aggrQuery.lastProgress)

    //We need that for sure
    aggrQuery.awaitTermination()

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

