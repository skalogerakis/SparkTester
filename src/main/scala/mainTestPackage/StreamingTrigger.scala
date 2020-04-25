package mainTestPackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}

object StreamingTrigger {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val retailDataSchema = new StructType()
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", DateType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamData = spark.readStream
      .schema(retailDataSchema)
      .option("maxFilesPerTrigger","2")
      .csv("/Users/talentorigin/temp_working")

    val tumblingWindowAggregations = streamData
      .groupBy(
        window(col("InvoiceTimestamp"), "1 hours", "15 minutes"),
        col("Country")
      )
      .agg(sum(col("UnitPrice")))

    /**
     *  Three choices continuous, once, processing time
     *  Processing Time: trigger every time(10 seconds) etc
     *  Once: Works as normal spark job. It will not wait indefinitely but it will terminate(ex we have a cluster and we want to terminate after everything is done)
     *  Continuous: EXPERIMENTAL takes ms to process
     */
    /**
     * NOTE: 1. If the previous microbatch is not completed
     * with in the time interval, then the engine will wait
     * until the interval is over before going to next
     * 2.
     */
    val sink = tumblingWindowAggregations
      .writeStream
      .trigger(Trigger.Once())
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()

    sink.awaitTermination()
  }
}