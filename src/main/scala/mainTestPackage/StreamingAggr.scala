package mainTestPackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._

object StreamingAggr {
  //This is how main class is defined in scala
  def main(args: Array[String]): Unit = {
    //This is how structured streaming starts, by building a spark session
    val sparkSession = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()

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
    val streamingData = sparkSession.readStream
      .schema(retailDataSchmema)
      .option("maxFilesPerTrigger","2")  // This option simply makes spark to read up to 2 files per batch
      .csv("/home/skalogerakis/TUC_Projects/SparkTest/MyFiles/TestData")  //Specify directory of file to read

    //This is where we apply our aggregations. TODO read whole Spark Scala API and search functions to check all available aggregations
    val aggr = streamingData
        .filter("Quantity > 10")
        .groupBy("InvoiceDate", "Country")
        .agg(sum("UnitPrice"))

    val aggrQuery = aggr.writeStream
        .format("console")
        .outputMode(OutputMode.Complete())
        .start()


    aggrQuery.awaitTermination()


  }

}
