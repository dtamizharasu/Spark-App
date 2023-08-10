package org.fileoperations

import org.config.SparkConfig

object ReadCsv extends App{

  val spark = SparkConfig.sparkSession()
  val output_Path = "C:\\Users\\dhta2003\\IdeasProject\\Spark-app\\src\\test\\resources\\output\\csv\\final.csv"
  var csv = spark.read
    .option("header","true")
    .option("delimiter",",")
    .option("inferaSchema","false")
    .format("csv")
    .load(output_Path) // load or csv
  csv.show()

  spark.stop()

}
