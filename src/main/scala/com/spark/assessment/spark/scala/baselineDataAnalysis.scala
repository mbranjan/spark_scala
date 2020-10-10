package com.spark.assessment.spark.scala

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.apache.spark.sql.AnalysisException

object baselineDataAnalysis {

	val log = Logger.getLogger(this.getClass.getName)

			def main(args: Array[String]):Unit ={   


					val configFilePath = "src/main/resources/config_file.conf"

							log.warn("***********Creating Spark Session Object***********")

							val spark = SparkSession.builder()
							.master("local[*]")
							.appName("Baseline data Analysis") 
							.getOrCreate() 

							log.warn("***********Creating Operations Class Object***********")
							val opsObj = new Operations()


							log.warn("*******Calling function readinConfigurationFile **********")							
							val productList = opsObj.readingConfigurationFile(configFilePath).asScala.toList


							log.warn("********* Calling function listOfDataframe **************")
							val listOfDf = opsObj.listOfDataframe(spark:SparkSession, productList:List[String])


							log.warn("********* Calling function joinDataFrame **************")
							val finalDF = opsObj.joinDataFrame(listOfDf)


							log.warn("\n\n##########################################    DISPLAYING FINAL RESULT   #########################################\n")
							finalDF.show()

	}    

}