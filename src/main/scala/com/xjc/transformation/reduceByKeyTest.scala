package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}

/**
reduceByKey(func, [numTasks])   

When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
*/

object reduceByKeyTest {

      implicit def int2str(num:Int):String = String.valueOf(num)
      def main(args:Array[String]) {
              val conf = new SparkConf().setAppName("MapPartitionsTest").setMaster("local[2]")
              val sc = new SparkContext(conf)
              val data = 1 to 10
              val rdd = sc.parallelize(data, 2)
              val rdd1 = rdd.map(_ * 2).union(rdd).map((i) =>(i.toString, 1))
              rdd1.reduceByKey((a,b) => a + b).collect.foreach(println)

              val file = "file:///home/xjc/abc.txt"
              val fileRdd = sc.textFile(file)
              val rdd3 = fileRdd.flatMap(_.split("\\s")).map(i=>(i.length, 1))
              rdd3.reduceByKey(_ + _).sortBy(_._2, false, 1).foreach(println)
              rdd3.reduceByKey(_ + _).map(a=>(a._2,a._1)).sortByKey(false, 1).map(a=>(a._2,a._1)).foreach(println)

              val rdd4 = fileRdd.flatMap(_.split("\\s")).map(i=>(i,1))
              rdd4.reduceByKey(_ + _).sortBy(_._2, false, 1).foreach(println)
              rdd4.reduceByKey(_ + _).map(a=>(a._2,a._1)).sortByKey(false, 1).map(a=>(a._2,a._1)).foreach(println)
              //rdd3.reduceByKey(_ + _).map((i,j)=>(j,i)).sortByKey(false, 1).map((i,j)=>(j,i)).foreach(println)
              
              sc.stop
  }
}

/**
(4,6)
(1,5)
(3,3)
(5,2)
(6,1)
(8,1)
(7,1)

*/


/**
(i,3)
(plan,2)
(because,1)
(have,1)
(never,1)
(when,1)
(b,1)
(some,1)
(plans,1)
(worried,,1)
(not,1)
(was,1)
(had,1)
(a,1)
(fit,,1)
(always,1)


*/
