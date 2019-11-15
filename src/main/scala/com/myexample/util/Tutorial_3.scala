package com.myexample.util

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

//read a json data and create a dataframe
object Tutorial_3 {

  def main(args:Array[String])={

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

    val sc=SparkSession
      .builder()
      .appName("'")
      .master("local")
      .getOrCreate()

    import sc.implicits._

   /* We want to answer two questions:
      What are the best-selling and the second best-selling products in every category?
    What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product?*/

    sc.sparkContext.setLogLevel("ERROR")

    //reading json
val datajson: DataFrame =sc.read.json("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\empdata.json")
    datajson.show()

    /*val datapro=sc.sparkContext.textFile("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\product_revenue.txt")
        .map(_.split(",")).map(e=>Row(e(0),e(1),e(2)))
    */

    val datawithschema: DataFrame =sc.read.option("header",true)
      .csv("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\product_revenue.txt")

    val data =sc.read.option("header",true)
      //.load("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\product_revenue.txt")
        .csv("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\product_revenue.txt").filter("revenue > 3000").filter("category = 'tablet'")

    //val datad=sc.read.load("")
      //.schema()

    datawithschema.show()
    data.show()

    datawithschema.createOrReplaceTempView("product_revenue")
    sc.sql("select product from product_revenue ").show()

    //get list of columns from dataframe
    datawithschema.select("product","revenue").show()

    //get distinct record dataframe
    datawithschema.select("category").distinct().show()


    /*
        +---------+
        | category|
        +---------+
        |   tablet|
        |cellphone|
        +---------+
    */

    //Category wise revenue
    datawithschema.select("category","revenue").groupBy("category").agg("revenue"->"sum").show()

    /*+---------+------------+
    | category|sum(revenue)|
      +---------+------------+
    |   tablet|     20500.0|
      |cellphone|     23000.0|
      +---------+------------+*/

    datawithschema.select("category","revenue").groupBy("category").agg("revenue"->"max","revenue"->"min").show()




  }

}
