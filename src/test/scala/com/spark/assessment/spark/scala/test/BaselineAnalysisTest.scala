package com.spark.assessment.spark.scala.test

import com.spark.assessment.spark.scala.baselineDataAnalysis 
import com.spark.assessment.spark.scala.Operations

class BaselineAnalysisTest extends EnvironmentInitializer {

	/*
	 * sample test cases are written here. 
	 * Many other test cases can be implemented depending on scenarios.
	 */


	test("TestCase 1: Validating the Number of Dataframes") {	  

		val obj = new Operations
				val product_list = List("barley","beef", "corn", "cotton", "pork","rice", "wheat")
				val list_of_df = obj.listOfDataframe(ss, product_list)

				assert(list_of_df.length == 7)

	}


	test("TestCase 2: Validating the Record count") {

		val obj = new Operations
				val product_list = List("barley","beef", "corn", "cotton", "pork","rice", "wheat")
				val list_of_df = obj.listOfDataframe(ss, product_list)    
				val final_df = obj.joinDataFrame(list_of_df)

				//    final_df.show
				assert(final_df.count == 13)


	}


}