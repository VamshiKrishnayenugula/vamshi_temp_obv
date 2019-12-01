package com.ica


import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.fs.{FileSystem, Path}


object TempObeservations {
  val spark = SparkSession.builder.master("local")
    .appName("temp_observations")
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  //Main function
  def main(args: Array[String]): Unit = {
    //input and output directories
   val inputDir= "./src/main/resources/temp_obs.txt"
   val outputDir= "./src/main/resources/result"

    tempobservation(inputDir,outputDir)
  }

  //creating a function to read and write to HDFS files
  def tempobservation(inputDir: String, outputDir: String): Unit = {
    import spark.implicits._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val rdd = spark.sparkContext.textFile(inputDir)
    val df = rdd.map(row => row.trim.replaceAll(" +", " "))
      .map(_.split(" "))
      .map(c => (c(0), c(1), c(2), c(3), c(4), c(5)))
      .toDF("year", "c1", "c2", "c3", "c4", "c5")

    df.createOrReplaceTempView("temp_observations")

    val count = spark.sql("select count(*) from temp_observations")


    val outPutPath = new Path(outputDir)

    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)

    df.distinct().write.csv(outputDir)


  }


}
