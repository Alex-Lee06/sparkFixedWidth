package readData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.catalyst.ScalaReflection

object siteImpactData {
  def siteDataframe(siteFile: String): Unit = {

//    creating a spark session
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


      case class default(sid : Int, pid : Int, email : String)

// providing access key and secret key.  Will be moving this to properties files for security
    val accessKeyId = ""
    val secretAccessKey = ""
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secretAccessKey)

    import spark.implicits._

    //used to read csv files
    val siteDf = spark.read.option("header", "true").csv(siteFile)
//    siteDf.show(5)

    val firstName = "fname"
    val lastName = "lname"
    siteDf.select(firstName, lastName, "sid", "pid").show()

//    siteDf.write.mode("append").format("csv").save("")



  }//end of def
}
