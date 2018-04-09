package com.yuantek.spark

import org.apache.spark.sql.SparkSession


object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///Users/mizhe/spark/info.text")
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt,line(1),line(2).toInt)).toDF()
    infoDF.show()
    infoDF.filter(infoDF.col("age") > 19).show()

    infoDF.createOrReplaceTempView("info")
    spark.sql("select * from info where age > 19").show

    spark.stop()
  }

  case class Info(id: Int, name: String, age: Int)

}
