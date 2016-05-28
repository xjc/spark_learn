package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}

/**
  def fold(zeroValue: T)(op: (T, T) ⇒ T): T
  Aggregate the elements of each partition, and then the results for all the partitions, using a given associative and commutative function and a neutral "zero value".
 */

object foreachPartitionTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("foreachPartitionTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            val data = 1 to 10
            val rdd = sc.parallelize(data, 2)    

            implicit def int2str(a:Int):String = a.toString
            var num = 0
            rdd.foreachPartition(i=>{
                    num += 1
                    var list1 = List[String]() 
                    for ( j<- i) 
                        if (j % 2 == 0)
                            list1 = j :: list1
                    list1.foreach(println)
                    }
                    )

            println("partition size: " + num)
            sc.stop
    }
}
