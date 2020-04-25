package mainTestPackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object StreamingTransformations {

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
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    //readStream is a mechanism of reading a particular directory in streaming fashion
    val streamingData = sparkSession.readStream.schema(retailDataSchmema)
      .csv("/home/skalogerakis/TUC_Projects/SparkTest/MyFiles/TestData")  //Specify directory of file to read

    val filteredData = streamingData
      .filter("Country = 'France'")
      .where("Quantity > 1")
      .drop("CustomerId")
      .select("InvoiceNo","StockCode", "Description","Quantity")
      .writeStream
      .queryName("SalesDetails")
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    filteredData.awaitTermination()


  }
}
