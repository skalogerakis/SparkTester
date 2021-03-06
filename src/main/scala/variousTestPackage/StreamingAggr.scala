package variousTestPackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._

object StreamingAggr {
  //This is how main class is defined in scala
  def main(args: Array[String]): Unit = {
    //This is how structured streaming starts, by building a spark session
    val sparkSession = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()

    //This hides too much log information and sets log level to error
    //sparkSession.sparkContext.setLogLevel("ALL")

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
      .option("maxFilesPerTrigger","2")  // This option simply makes spark to read up to 2 files per batch
      .csv("/home/skalogerakis/TUC_Projects/SparkTest/MyFiles/TestData")  //Specify directory of file to read

    //This is where we apply our aggregations. TODO read whole Spark Scala API and search functions to check all available aggregations
    val aggr = streamingData
        .filter("Quantity > 10")
        .groupBy("InvoiceDate", "Country")
        .agg(avg("UnitPrice"))

    /**
     * Whenever there is checkpoint directory attached to the query, spark goes through the content of the
     * directory before it accepts new data. This makes sure that spark revovers the old state before it starts
     * processing new data. So whenever there is restart, spark first recovers the old state and then start processing new data from the stream.
     */
    val aggrQuery = aggr.writeStream

        .format("console")
        //.option("path", "/home/skalogerakis/TUC_Projects/SparkTest/Output") //TODO add this when added in real file
        .outputMode(OutputMode.Complete())
        .option("checkpointLocation", "/home/skalogerakis/TUC_Projects/SparkTest/Checkpoint/")
        .trigger(Trigger.Once)
        .queryName("AggrExample")
        .start()


//    import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
//    val engine = aggrQuery.asInstanceOf[StreamingQueryWrapper].streamingQuery
//    import org.apache.spark.sql.execution.streaming.StreamExecution
//    assert(engine.isInstanceOf[StreamExecution])
//
//    import org.apache.spark.sql.execution.streaming.MicroBatchExecution
//    val microBatchEngine = engine.asInstanceOf[MicroBatchExecution]
//    assert(microBatchEngine.trigger == Trigger.Once)

    aggrQuery.awaitTermination()


  }

}
