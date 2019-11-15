package com.myexample.util

import org.apache.spark.sql.SparkSession

object demogroup {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

    val sc = SparkSession
      .builder()
      .appName("sparkcassandra")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")


    val data=sc.read.format("csv")
      .option("header","true")
      .load("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\train_data.csv")

    data.show()

    val abc=data.first().getString(3)
    println(abc)
    data.createOrReplaceTempView("train_data")

    val newdata=sc.sql("select trainaffected,delay,delaycodedesc,sum(cast(cost as integer)) as cost,region from train_data group by trainaffected,delay,delaycodedesc,region")
    newdata.show()

  }

}

