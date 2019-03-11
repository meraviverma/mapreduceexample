package com.myexample.util

import org.apache.spark.sql.SparkSession

/*
Q) we have a text file as 1,ravi@gaya id,name@city .
create a dataframe and store the data in the cassandra.
 */
object Tutorial_2 {

  def main(arg: Array[String]):Unit={

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")
    val sc=SparkSession
      .builder()
      .appName("CassandraExample")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")

    val abc=Seq(1,"ravi")
    val data=sc.sparkContext.parallelize(abc)
    println(data.collect().mkString(","))
    //data.foreach(println)

    val data2=sc.sparkContext.textFile("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\Employee.txt")

    data2.foreach(println)


  }
}
