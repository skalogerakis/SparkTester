package variousTestPackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}

object TumblingWindow {
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

    val tumblingWindow = streamingData
      .groupBy( //apply window operations in streaming. MUST use a column with timestamp and time frame
        window(
          col("InvoiceTimeStamp"),"1 hour"
        ), //mention time in lower case letters
        col("Country")
      )
      .agg(sum(col("UnitPrice")))

    val outputMode = tumblingWindow
        .writeStream
        .format("console")
        .option("truncate","false")
        .outputMode("complete")
        .start()


    outputMode.awaitTermination()


  }
}
