package readData

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{explode, first, lit, monotonically_increasing_id, udf}
import org.apache.spark.sql._
import readProperties.jsonReader
//import readProperties.jsonData
import com.quartethealth.spark.fixedwidth.FixedwidthContext
import com.quartethealth.spark.fixedwidth.readers._
import com.quartethealth.spark.fixedwidth._
import scala.collection.mutable.ListBuffer


object bmiData {
  def bmiDatarame(bmiFile : String) = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL BMI data")
      .config("spark.master", "local")
      .getOrCreate()


    import spark.implicits._


    val accessKeyId = ""
    val secretAccessKey = ""
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secretAccessKey)

    val jsonFile = "appProperties.json"

    val jsonDf = spark.read.option("multiline", "true").json(jsonFile)
    val dfColInfo = jsonDf.select(explode(jsonDf("columnDefinition"))).toDF("columnInfo")
    dfColInfo.show()
    val seperateDf = dfColInfo.select("columnInfo.columnName",  "columnInfo.columnID", "columnInfo.length",
          "columnInfo.startPoint", "columnInfo.endPoint")
    seperateDf.show()
    val dataTypeChange = seperateDf.withColumn("length", 'length.cast("Int"))
    val lengths = dataTypeChange.select("length").as[Int].collect().toSeq

    println(lengths)




    val colNameList = seperateDf.select("columnName").collect().map(_.toString()).toSeq

//    val lengths = Seq(3,10,5,4)

    def parseLinePerFixedLengths(line: String, lengths: Seq[Int]): Seq[String] = {
      lengths.indices.foldLeft((line, Array.empty[String])) { case ((rem, fields), idx) =>
        val len = lengths(idx)
        val fld = rem.take(len)
        (rem.drop(len), fields :+ fld)
      }._2
    }

    val bmiDf = spark.read.textFile(bmiFile)

    val fields = bmiDf.
      map(parseLinePerFixedLengths(_, lengths)).
      withColumnRenamed("value", "fields")

    val answer = lengths.indices.foldLeft(fields) { case (result, idx) =>
      result.withColumn(s"col_$idx", $"fields".getItem(idx))
    }

    answer.drop("fields").toDF(colNameList : _*).show()
//
//
//    val bmiDf = spark.read.textFile(bmiFile)







//    .map(l => (l.substring(0, 18).trim(), l.substring(18, 30).trim(), l.substring(30,42).trim(), l.substring(42, 54).trim(), l.substring(54,69).trim(),
//    l.substring(69,70).trim(), l.substring(70,90).trim(), l.substring(90,92).trim(), l.substring(92,93).trim(), l.substring(93,94).trim(), l.substring(94,96).trim(),
//    l.substring(96,102).trim(), l.substring(102,104).trim(),l.substring(104,114).trim(),l.substring(114,116).trim(),l.substring(116,144).trim()))
//    .toDF("lemsmatchCode","individualid", "familyid","locationid","firstname","middleinitial","lastname","lastnamesuffix","titlecode", "gender", "age",
//    "birthdate", "birthdate_dd", "housenumber","streetpredirectional","streetname").show()


//    }
//  }


//
//    val bmiDf = spark.read.textFile(bmiFile)
//    .map(l => (l.substring(0, 18).trim(), l.substring(18, 30).trim(), l.substring(30,42).trim(), l.substring(42, 54).trim(), l.substring(54,69).trim(),
//    l.substring(69,70).trim(), l.substring(70,90).trim()))
//    .toDF(colNameList:_*).show()























//    val mapBmi = bmiDf.














//    val bmiDf = spark.read.textFile(bmiFile)
//      .map(l => (l.substring(i, 18)))
//      .toDF(colNameList : _*).show()
























// Fix without changing actually column type.
//    val getNull = udf(() => None: Option[String])
//    val newBmiDf = bmiDf.withColumn("lastnamesuffix", getNull())
//    val newBmiDf1 = newBmiDf.withColumn("streetpredirectional", getNull())
////    newBmiDf.printSchema()
//    newBmiDf.show(5)
//    newBmiDf1.show(5)



  }// end of def
}//end of object













//    val mappedNameID = seperateDf.rdd.map(row => (row.getLong(1)-> row.getString(0))).collectAsMap()
////    println(mappedNameID)
//
//    val colLengthMapped = seperateDf.rdd.map(row => (row.getString(0) -> row.getLong(2))).collectAsMap()
////    println(colLengthMapped)
//



////need to adjust to read properties files for all column names and fixed width
//
//val bmiDf = spark.read.textFile(bmi_file)
//.map(l => (l.substring(0, 18).trim(), l.substring(18, 30).trim(), l.substring(30,42).trim(), l.substring(42, 54).trim(), l.substring(54,69).trim(),
//l.substring(69,70).trim(), l.substring(70,90).trim(), l.substring(90,92).trim(), l.substring(92,93).trim(), l.substring(93,94).trim(), l.substring(94,96).trim(),
//l.substring(96,102).trim(), l.substring(102,104).trim(),l.substring(104,114).trim(),l.substring(114,116).trim(),l.substring(116,144).trim()))
//.toDF("lemsmatchCode","individualid", "familyid","locationid","firstname","middleinitial","lastname","lastnamesuffix","titlecode", "gender", "age",
//"birthdate", "birthdate_dd", "housenumber","streetpredirectional","streetname")
//
//
//
//
//// Fix without changing actually column type.
//val getNull = udf(() => None: Option[String])
//val newBmiDf = bmiDf.withColumn("lastnamesuffix", getNull())
//val newBmiDf1 = newBmiDf.withColumn("streetpredirectional", getNull())
////    newBmiDf.printSchema()
//newBmiDf.show(5)
//newBmiDf1.show(5)





//Json parsing using spark

//    val jsonFile = "appProperties.json"
//
//    val jsonDf = spark.read.option("multiline", "true").json(jsonFile)
////    jsonDf.show()
//    val dfColInfo = jsonDf.select(explode(jsonDf("columnDefinition"))).toDF("columnInfo")
//    dfColInfo.show()
//    val seperateDf = dfColInfo.select("columnInfo.columnName",  "columnInfo.columnID", "columnInfo.length",
//      "columnInfo.startPoint", "columnInfo.endPoint")
//        seperateDf.show()
//    seperateDf.printSchema()



//    val colNameList = seperateDf.select("columnName").collect().map(_.toString()).toSeq
//    println(colNameList)




//    val startPoint = seperateDf.select("startPoint").show()
//    val startPoint = seperateDf.select("startPoint").map(r=>r.getLong(0)).collect()
//    val intStart = startPoint.map(_.toInt)
//    for(i <- intStart){
//      println(i)
//    }
//
//    val endPoint = seperateDf.select("endPoint").map(r=>r.getLong(0)).collect()
//    val intEnd = endPoint.map(_.toInt)
//    for(x <- intEnd){
//      println(x)
//    }
//
//  for(i<- intStart){
//    for(x<- intEnd){
//      val bmiDf = spark.read.textFile(bmiFile)
//        .map(l => (l.substring(i, x)))
//        .toDF(colNameList : _*)
