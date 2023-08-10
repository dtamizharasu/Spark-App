package org.fileoperations

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, count}
import org.config.SparkConfig

object CreateParquet extends App {

  val spark = SparkConfig.sparkSession()
  val data = Seq(("James ", "", "Smith", "36636", "M", 3000),
    ("Michael ", "Rose", "", "40288", "M", 4000),
    ("Robert ", "", "Williams", "42114", "M", 4000),
    ("Maria ", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", 1000),
    ("Jen", "Mary", "Brown", "", "F", 1000))

  val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")

  import spark.sqlContext.implicits._

  var df = data.toDF(columns: _*)
  df.show()
  df = df.groupBy("firstname").agg(count("firstname").alias("count"),
    functions.sum("salary").alias("sum"),
    avg("salary").alias("avg")
  )
  df.show()
  val outputPath = "C:\\Users\\dhta2003\\IdeasProject\\Spark-app\\src\\test\\resources\\output\\parquet\\people.parquet"
  df.coalesce(1)
    .write
    .format("parquet")
    .partitionBy("firstname", "avg")
    .mode("overwrite")
    .save(outputPath) // save or parquet

  spark.stop()
}
