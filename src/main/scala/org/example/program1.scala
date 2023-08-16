package org.example

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object program1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)  // Set root logger to ERROR level

    val spark = SparkSession.builder()
      .appName("SparkListExample")
      .master("local[*]")
      .getOrCreate()

    val inputList = List(1, 2, 3, 4, 5)
    val result = spark.sparkContext.parallelize(inputList)
      .map(_ * 2)
      .collect()

    result.foreach(println)

    spark.stop()
  }
}
