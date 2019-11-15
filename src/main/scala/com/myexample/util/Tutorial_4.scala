package com.myexample.util

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Tutorial_4 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

    val sc = SparkSession
      .builder()
      .appName("sparkcassandra")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")


    val somedata=Seq((1,"train become FOCO from 0700-03 N"),(2,"Train (PL) N "),(3,"09/2019su"))
    val datadf1=sc.sparkContext.parallelize(somedata).toDF("id","trn_comment")
    datadf1.printSchema()
    datadf1.createOrReplaceTempView("train_focus")
    datadf1.show(false)

    sc.sql("select substr(trim(trn_comment),-1,1), case when substr(trim(trn_comment),-1,1) = 'N' then 'Shutdown' else '' end as train_flag from train_focus").show()

    datadf1.printSchema()

  }

}
