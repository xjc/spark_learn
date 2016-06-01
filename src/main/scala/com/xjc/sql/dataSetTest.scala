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

case class Person(name: String, age: Long)
case class Person1(name:String, age:Long, sex:String, address:String)

object dataSetTest {

  // Encoders are also created for case classes.

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("dataSetTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new SQLContext(sc)
    //// Encoders for most common types are automatically provided by importing sqlContext.implicits._
    import ssc.implicits._

    val ds = Seq(1, 2, 3).toDS()
    ds.show

    val ds1 = Seq(Person("Andy", 32)).toDS()
    ds1.show

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
    val ds2 = ssc.read.json("file:///home/xjc/git/spark_learn/resource/people.json").as[Person1]
    ds2.show

    sc.stop
  }
}
