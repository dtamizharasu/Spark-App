package org.fileoperations

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.IntegerType
import org.config.SparkConfig


object CreateCsv extends App {

  val spark = SparkConfig.sparkSession()
  val columns = Seq("language", "user_count")
  val data = Seq(("Java", "20000"), ("Scala", "15000"), ("Python", "20000"))
  var df = spark.createDataFrame(data).toDF(columns: _*)
    .withColumn("user_count", col("user_count").cast(IntegerType))
  df.createTempView("users")
  df = spark.sql("select * from users where user_count > 14000")
  df.show()
  df = df.withColumn("country", when(col("user_count") > 16000, "india")
    .otherwise("usa"))
    .withColumn("total", col("user_count") * 5)
  df.show()
  df = df.groupBy("language").pivot("country").sum("user_count")
  df.show()

  val output_Path = "C:\\Users\\dhta2003\\IdeasProject\\Spark-app\\src\\test\\resources\\output\\csv\\final.csv"
  df.coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ",")
    .save(output_Path) // load or csv

  spark.stop()


}
