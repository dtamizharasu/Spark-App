package org.config

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

object SparkConfig {

  def sparkSession(): SparkSession = {

    PropertyConfigurator.configure("src//main//resources//log4j.properties")
    //    System.setProperty("hadoop.home.dir", "C:\\Users\\spark-3.3.1-bin-hadoop3")


    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark-App")
      //      .config("spark.executor.memory", "5gb")
      //      .config("spark.cores.max", "6")
      .getOrCreate()

    println("First SparkContext")
    println("APP Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

    spark
  }

}
