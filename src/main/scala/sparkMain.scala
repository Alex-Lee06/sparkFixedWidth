import java.io.FileInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import readData.importData
import readData.bmiData
import readData.siteImpactData
import dedupSimilarName.dupSimilarName
import readProperties.jsonReader


import scala.io.Source


object sparkMain {
  def main(args: Array[String]): Unit = {


    //    reading json files for properties information
//    val jsonFile = "fruit_fixedwidth.txt"
//    jsonReader.properties_file(jsonFile)
//    ===============json reader====================

////    create dataframe for consumer data from s3 dev
//    val consumer_file = "s3a://site-impact-dirty-data/eDemo/REAL-TIME.txt"
//    val consDf = importData.consumer_dataframe(consumer_file)
//
//print("============Consumer Data=================")
//
//    create data for bmi data from s3 staging
    val bmiFile = "s3a://si-data-monster-raw-data-dev-env/ig/bmi01AL.txt"
    val bmiDf = bmiData.bmiDatarame(bmiFile)
//
//    bmiData.bmi_dataframe()
//print("============BMI data==================")
//
//
////    create dataframe for SI from s3 staging
//    val si_file = "s3a://site-impact-dirty-data/Bulldogs/PID31101_20181011_390452_ALL_TaxDefense.csv"
//    siteImpactData.siteDataframe(si_file)

//print("============SI data===========")
//
//    //dropping duplicate rows based on specific columns
//
//    val duplicateFile = "REAL-TIME.txt"
//    val dedupDF = dup_SN.dedup_SN(duplicateFile)



//    val jsonTester = "appTest.json"
//    jsonReader.properties_file(jsonTester)



  }//end of main
}//end of object
