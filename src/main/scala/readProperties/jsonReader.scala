package readProperties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, first, from_json, get_json_object, lit, monotonically_increasing_id, schema_of_json, udf}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}



object jsonReader {
  def properties_file(jsonFile: String): Unit = {

//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL BMI data")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//    val jsonDf = spark.read.option("multiline", "true").json(jsonFile)
//    val dfColInfo = jsonDf.select(explode(jsonDf("columnDefinition"))).toDF("columnInfo")
//    dfColInfo.show()
//    val seperateDf = dfColInfo.select("columnInfo.columnName",  "columnInfo.columnID", "columnInfo.length",
//      "columnInfo.startPoint", "columnInfo.endPoint")
//    seperateDf.show()
//    val dataTypeChange = seperateDf.withColumn("length", 'length.cast("Int"))
//    val lengths = dataTypeChange.select("length").as[Int].collect().toSeq
//
//    println(lengths)


  }
}

