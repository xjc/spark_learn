package com.xjc.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.Row

/**
 */

case class Record(str1: String, str2: String, str3:String, num:Int)

object rdd2DataFrameTest {

  // Encoders are also created for case classes.

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("rdd2DataFrameTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new SQLContext(sc)
    //// Encoders for most common types are automatically provided by importing sqlContext.implicits._
    import ssc.implicits._
    val rdd1 = sc.parallelize((1 to 10).map(i=>("a_" + i, "b_" + i, "c_" + i, i)))

    //plan a: Inferring the Schema Using Reflection
    val df1 = rdd1.map(i=>Record(i._1, i._2, i._3, i._4)).toDF
    df1.show
    println("#" * 100)
    val df2 = rdd1.toDF("str1", "str2", "str3", "num")
    df2.show
    println("#" * 100)
    df2.registerTempTable("t")
    ssc.sql("select * from t").show
    println("#" * 100)
    
    //plan b: Programmatically Specifying the Schema
    val schema = StructType(Array(StructField("str1", StringType, true), StructField("str2", StringType, true), StructField("str3", StringType, true), StructField("num", IntegerType, true)))
    val rdd1_1 = rdd1.map(i=>Row(i._1, i._2, i._3, i._4))
    val df3 = ssc.createDataFrame(rdd1_1, schema)
    println("!" * 100)
    df3.show


    sc.stop
  }
}
