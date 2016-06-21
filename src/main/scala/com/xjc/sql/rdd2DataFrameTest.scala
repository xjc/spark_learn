package com.xjc.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.Row

/**
 */

case class Record(str1: String, str2: String, str3:String, num:Int)
case class C1(str1:String, str:String, num:String)

object rdd2DataFrameTest {

  // Encoders are also created for case classes.

  def main(args:Array[String]) {
    //val conf = new SparkConf().setAppName("rdd2DataFrameTest").setMaster("local[2]")
    val conf = new SparkConf().setAppName("rdd2DataFrameTest")
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

    //plan c
    val df4 = ssc.createDataFrame(rdd1)
    df4.printSchema
    /**
     * root
     *  |-- _1: string (nullable = true)
     *   |-- _2: string (nullable = true)
     *    |-- _3: string (nullable = true)
     *     |-- _4: integer (nullable = false)
     */
    val df4_1 = ssc.createDataFrame(rdd1).toDF("a", "b", "c", "d")
    df4_1.printSchema

    val rdd1_2 = rdd1.map(i=>Record(i._1, i._2, i._3, i._4))
    val df4_2 = ssc.createDataFrame(rdd1_2)
    df4_2.printSchema

    //plan d  from hive
    //val df5 = ssc.table("""app.t20160602""")
    //df5.printSchema
    /* root
     *  |-- name: string (nullable = true)
     *   |-- id: string (nullable = true)
     */

    val hc = new HiveContext(sc)
    val df_hive = hc.sql("""select * from app.t20160602""")
    df_hive.printSchema
    df_hive.show
    

    df4_2.registerTempTable("temp")
    val rdd3 = ssc.sql("select str1, str2, num from temp").
        map(i=>(i.getString(0), i.getString(1), i.getInt(2))).collect
    //res84: Array[(String, String, Int)] = Array((a_1,b_1,1), (a_2,b_2,2), (a_3,b_3,3), (a_4,b_4,4), (a_5,b_5,5), (a_6,b_6,6), (a_7,b_7,7), (a_8,b_8,8), (a_9,b_9,9), (a_10,b_10,10))
    val rdd3_1 = ssc.sql("select str1, str2, num from temp").
        map(i=>(i.getAs[String](0), i.getAs[String](1), i.getAs[Int](2))).collect
    //rdd3_1: Array[(String, String, Int)] = Array((a_1,b_1,1), (a_2,b_2,2), (a_3,b_3,3), (a_4,b_4,4), (a_5,b_5,5), (a_6,b_6,6), (a_7,b_7,7), (a_8,b_8,8), (a_9,b_9,9), (a_10,b_10,10))
    val rdd3_2 = ssc.sql("select str1, str2, num from temp").
       map(i=>(i.get(0), i.get(1), i.get(2))).collect
    //rdd3_2: Array[(Any, Any, Any)] = Array((a_1,b_1,1), (a_2,b_2,2), (a_3,b_3,3), (a_4,b_4,4), (a_5,b_5,5), (a_6,b_6,6), (a_7,b_7,7), (a_8,b_8,8), (a_9,b_9,9), (a_10,b_10,10))
    
    //fusion
    import org.apache.spark.sql.Row
    val rdd3_3 = ssc.sql("select str1, str2, num from temp").
      map{case Row(a,b,c) => (a,b,c)}
    //res104: org.apache.spark.rdd.RDD[(Any, Any, Any)] = MapPartitionsRDD[150] at map at <console>:28


    val rdd3_3_df = rdd3_3.map(i=>C1(i._1.toString,i._2.toString,i._3.toString)).toDF
    rdd3_3_df.registerTempTable("temp1")
    ssc.sql("select * from temp1").show
    
    sc.stop
  }
}
