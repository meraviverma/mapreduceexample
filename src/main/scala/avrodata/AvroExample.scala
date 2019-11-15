package avrodata

import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._

object AvroExample {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

    val sc = SparkSession
      .builder()
      .appName("sparkcassandra")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")

    val outputdir="C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\data"
    val inputdir="C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\data\\part-00000-2a9758bb-236a-474b-824e-28e8313bdb63-c000.avro"
    val df = Seq(
      (2012, 8, "Batman", 9.8),
      (2012, 8, "Hero", 8.7),
      (2012, 7, "Robot", 5.5),
      (2011, 7, "Git", 2.0)).toDF("year", "month", "title", "rating")

    df.show()

    //sc.read.avro("")

    //df.write.partitionBy("year").avro(outputdir)
    //df.write.avro(outputdir)
    //df.write.format("com.databricks.spark.avro").save("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\data\\test.avro")



    val dfavro = sc.read.format("com.databricks.spark.avro").load(inputdir)
    dfavro.show()
   /* +-----+------+
    |title|rating|
    +-----+------+
    |  Git|   2.0|
      +-----+------+*/

    /*val dfavro = sc.read.avro(inputdir)

    dfavro.printSchema()
    dfavro.filter("year = 2011").collect().foreach(println)*/
  }

  }
