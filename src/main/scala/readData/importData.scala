package readData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object importData {
  def consumer_dataframe(consumerFile: String): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    import spark.implicits._
    val accessKeyId = "AKIAJSV6MXE6AAHMKZXQ"
    val secretAccessKey = "fARN8Tq+VEjDECy20yGJVM0NLuxWubsIoo24Tl1/"
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secretAccessKey)

    // used to create dataframe with encoding to windows-1252 and delimiter and reading textfile as csv file
    // since headers are provided this can be read as a csv file adding heads as true.
    val consumerDf = spark.read.option("header", "true").option("charset", "windows-1252").option("delimiter", "¦").csv(consumerFile)

    //shows the dataframe
    consumerDf.show(5)
//    consumerDf.write.mode("append").format("csv").save("s3a://sidm-spark-processed-data/testData")

  }
}
