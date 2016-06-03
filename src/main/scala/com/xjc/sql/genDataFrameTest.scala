package com.xjc.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import java.net.URI
import java.io.File

/**
 * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
 * people.json
 * {"name":"charlie", "age":20, "sex":"male"}
 * {"name":"john", "age":30, "sex":"male", "address":["New York", "Can"]}
 * {"name":"marry", "age":29, "sex":"female", "address":"chi"}
 */

object genDataFrameTest {

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("genDataFrameTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new SQLContext(sc)

    //method a
    val df = ssc.read.json("file:///home/xjc/git/spark_learn/resource/people.json")
      df.show

      df.printSchema
      /**
       * root
       *  |-- address: string (nullable = true)
       *  |-- age: long (nullable = true)
       *  |-- name: string (nullable = true)
       *  |-- sex: string (nullable = true)
       */


      val (c1, c2, c3) = (df("address"), df("age"), df("name"))  //org.apache.spark.sql.Column
      df.select(c1, c2, c3, c1, c2, c3).show
      df.select("name", "age", "address", "name", "age", "address").show

      df.filter(df("age") > 20).show

      df.groupBy("age").count.show
      /**
       *|age|count|
       +---+-----+
       | 20|    1|
       | 29|    1|
       | 30|    1|
       +---+-----+
       */

      //parquet test
      val fs = FileSystem.get(new URI("hdfs://david-xiao:9000"), new Configuration())
        val loc = "hdfs://david-xiao:9000/tmp/20160601"
        //FileUtil.fullyDelete(new File("/tmp/20160601"))
        fs.delete(new Path(loc))
        df.saveAsParquetFile(loc)

        val df2 = ssc.read.parquet(loc)
        df2.show

      //mysql test
      import java.util.Properties
      val p = new Properties
      p.put("user", "xjc")
      p.put("password", "jie0512")
      val mysql_url = "jdbc:mysql://127.0.0.1:3306/web?useUnicode=true&characterEncoding=UTF-8"
      val table_name = "t_user_v2_20151103"
      val jdbcDF = ssc.read.jdbc(mysql_url, table_name, p)
      jdbcDF.show
      jdbcDF.groupBy("name").count.show


      //method b org.apache.spark.sql.DataFrameReader
      //val dfr = ssc.read.format("json")
      val dfr = ssc.read
      val df1 = dfr.format("json").load("file:///home/xjc/git/spark_learn/resource/people.json")
      df1.show
      val df3 = dfr.format("parquet").load("hdfs://david-xiao:9000/tmp/20160601")
      df3.show

      //column renamed toDF(colNames: String*): DataFrame
      //rdd.toDF()  // this implicit conversion creates a DataFrame with column name _1 and _2
      df3.toDF("a", "b", "c", "d").show

      sc.stop
  }
}
