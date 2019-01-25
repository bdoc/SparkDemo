package com.num3rd.spark.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Hive Demo")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("show databases").show()
  }
}
