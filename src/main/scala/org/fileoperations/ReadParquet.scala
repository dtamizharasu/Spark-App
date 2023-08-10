package org.fileoperations

import org.config.SparkConfig

object ReadParquet extends App {

  val spark = SparkConfig.sparkSession()
  val outputPath = "C:\\Users\\dhta2003\\IdeasProject\\Spark-app\\src\\test\\resources\\output\\parquet\\people.parquet"
  var parquetdf = spark.read
                  .format("parquet")
                  .option("header", "true")
                  .option("megaSchema","true")
                  .load(outputPath) // parquet or load
      parquetdf.show()

  spark.stop()

}
