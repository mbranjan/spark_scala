package com.spark.assessment.spark.scala.test


import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession

trait EnvironmentInitializer extends FunSuite with Matchers with BeforeAndAfterAll {

	private val master = "local[*]"
			private val appName = "Baseline Analysis TEST"

			var ss:SparkSession = _

			override def beforeAll(){

		ss = SparkSession.builder.appName(appName).master(master).getOrCreate()

	}

	override def afterAll() {

		if (ss != null) {

			ss.stop()

		}

	}

}
