package com.spark.assessment.spark.scala

import org.apache.log4j.{Level,Logger}
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._


class Operations {


	def readingConfigurationFile(configFilePath:String) = {

			/** 
			 *  In this method, we are reading values from config file  
			 *  
			 */	  


			val confReader = ConfigFactory.parseFile(new File(configFilePath))

					val productList = confReader.getStringList("productList")

					(productList)
	}


	def listOfDataframe(spark:SparkSession,productList:List[String]):List[DataFrame]={

			/** 
			 *  In this method, we are iterating through a list with two fuctions which returns 
			 *  a list of dataframes. 
			 *  
			 */

			val listOfDf = productList
					.map(product => (product,createSourceDF(spark, product)))
					.map(tup => createDataFrame(spark,tup._1,tup._2))
					//			.reduce((x,y)=> x.join(y,x("crop_year") === y("crop_year"),"inner"))

					return listOfDf
	}


	def createSourceDF(spark:SparkSession, file_name:String ):DataFrame = {

			/** 
			 *  In this method, we are creating multiple dataframes from 
			 *  different source files.  
			 *  
			 */

			val file_path = "src/test/resources/" + file_name

					val df = spark.read
					.option("sep","\t")
					.option("header","true")
					.csv(file_path).select("Crop_year","Production","Country")

					return df

	}  


	def createDataFrame(spark:SparkSession,product_name:String ,df:DataFrame):DataFrame = {

	  /** 
			 *  In this method, we are creating multiple intermediate dataframes  
			 *  from different source DFs.  
			 *  
			 */
	  
			df.createOrReplaceTempView("product_data")

			val column1 = product_name + "_year"
			//			val column1 = "crop_year"
			val column2 = "world_" + product_name + "_harvest"
			val column3 = "usa_" + product_name + "_contribution_percentage"

			val intermidiate_df = spark.sql(s"select res.Crop_year as $column1, cast(res.world_production as Long) as $column2, res.percentage as $column3 from (select Crop_year, production, sum(Production) over(partition by Crop_year) as world_production, round((production * 100 / sum(Production) over(partition by Crop_year)),2) as percentage, country from product_data  order by crop_year) res where country='USA'")


			return intermidiate_df
	}


	def joinDataFrame(listOfDf:List[DataFrame]):DataFrame =  {

	  /** 
			 *  In this method, we are creating joining intermediate dataframes  
			 *  to get finalDF.  
			 *  
			 */
	  
	  
			val barleyDF = listOfDf(0)
					val beefDF = listOfDf(1)
					val cornDF =  listOfDf(2) 
					val cottonDF = listOfDf(3)
					val porkDF = listOfDf(4)
					val riceDF = listOfDf(5)
					val wheatDF = listOfDf(6)

					val finalDF = barleyDF.join(beefDF, barleyDF("barley_year") === beefDF("beef_year"),"inner")
					.join(cornDF, barleyDF("barley_year") === cornDF("corn_year"),"inner")
					.join(cottonDF, barleyDF("barley_year") === cottonDF("cotton_year"),"inner")
					.join(porkDF, barleyDF("barley_year") === porkDF("pork_year"),"inner")
					.join(riceDF, barleyDF("barley_year") === riceDF("rice_year"),"inner")
					.join(wheatDF, barleyDF("barley_year") === wheatDF("wheat_year"),"inner")
					.drop("beef_year","corn_year","cotton_year","pork_year","rice_year","wheat_year")
					.withColumnRenamed("barley_year","year")

					return finalDF

	}

}