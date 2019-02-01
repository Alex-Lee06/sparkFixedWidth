package dedupSimilarName

import readData.importData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object dupSimilarName {
  def dedupSN(dup_file: String): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark duplication example")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

//    Unprocessed duplications
    val dupFile = spark.read.option("header", "true").option("delimiter", "�").csv(dup_file)
    dupFile.show()

//    Drops duplicates using specified columns as identifiers of similarities
    val withoutDuplicates = dupFile.dropDuplicates(Seq("FIRSTNAME", "EMAILADDRESS")).show()

//    Can also use distinct()
//    dupFile.distinct().show()



  }//end of def
}












//    val accessKeyId = ""
//    val secretAccessKey = ""
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", accessKeyId)
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secretAccessKey)
//    import spark.implicits._
//    val siteDf = spark.read.option("header", "true").csv(site_file)
//    siteDf.show(5)
