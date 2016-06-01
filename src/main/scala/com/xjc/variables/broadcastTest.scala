package com.xjc.variables
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._

/**
 *
 * Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.
 * Spark actions are executed through a set of stages, separated by distributed “shuffle” operations. Spark automatically broadcasts the common data needed by tasks within each stage. The data broadcasted this way is cached in serialized form and deserialized before running each task. This means that explicitly creating broadcast variables is only useful when tasks across multiple stages need the same data or when caching the data in deserialized form is important.
 *
 * Broadcast variables are created from a variable v by calling SparkContext.broadcast(v). The broadcast variable is a wrapper around v, and its value can be accessed by calling the value method. The code below shows this:
 */


object broadcastTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("broadcastTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val broad_val = sc.broadcast(1 to 100)
    println(broad_val.value)
    
    /**
    val data1 = (1 to 1000000).par.map(i=>("a" + i -> i))
    val data2 = (for (i <- 1 to 1000000 if (i % 1000 == 1)) yield i * 2) .par.map(i=>("a" + i -> i))
    val data3 = (for (i <- 1 to 1000000 if (i % 1000 == 9)) yield i * 2) .par.map(i=>("a" + i -> i))
    */
    val data1 = (1 to 1000000).map(i=>("a" + i -> i))
    val data2 = (for (i <- 1 to 1000000 if (i % 1000 == 1)) yield i * 2) .map(i=>("a" + i -> i)).toMap
    val data3 = (for (i <- 1 to 1000000 if (i % 1000 == 9)) yield i * 2) .map(i=>("a" + i -> i))

    /**
     * Can not directly broadcast RDDs; instead, call collect() and broadcast the result (see SPARK-5063)
     */
    val rdd1 = sc.parallelize(data1)
    //val rdd2 = sc.parallelize(data2)
    val broad_data2 = sc.broadcast(data2)
    val rdd3 = sc.parallelize(data3)

    //no shuffle
    val rdd_result_0 = rdd1.mapPartitions{ iter => { 
      val m =  broad_data2.value
      for ( (k,v) <- iter if (m.contains(k)) ) yield (k, (v, m.getOrElse(k, "")))
      }
    }
    println("rdd_result_0 count:" + rdd_result_0.count)
    println("rdd_result_0 dag:")
    println(rdd_result_0.toDebugString)
    println("#" * 50)

    /**
    val rdd_result_1 = rdd1.join(rdd2).map(i=>(i._1, i._2._1 + i._2._2)).sortByKey(false)
    println("rdd_result_1 max:" + rdd_result_1.max)
    println("rdd_result_1 dag:")
    println(rdd_result_1.toDebugString)
    println("#" * 50)
    */
    val rdd_result_2 = rdd1.join(rdd3).map(i=>(i._1, i._2._1 + i._2._2)).sortByKey(false)
    println("rdd_result_2 max:" + rdd_result_2.max)
    println("rdd_result_2 dag:")
    println(rdd_result_2.toDebugString)
    println("#" * 50)

    sc.stop
  }
}

/**
rdd_result_0 count:500
rdd_result_0 dag:
(2) MapPartitionsRDD[2] at mapPartitions at broadcastTest.scala:43 []
 |  ParallelCollectionRDD[0] at parallelize at broadcastTest.scala:37 []
##################################################
16/05/31 22:04:33 WARN TaskSetManager: Stage 1 contains a task of very large size (12588 KB). The maximum recommended task size is 100 KB.
rdd_result_2 max:(a998018,1996036)
rdd_result_2 dag:
(2) ShuffledRDD[9] at sortByKey at broadcastTest.scala:60 []
 +-(2) MapPartitionsRDD[6] at map at broadcastTest.scala:60 []
    |  MapPartitionsRDD[5] at join at broadcastTest.scala:60 []
    |  MapPartitionsRDD[4] at join at broadcastTest.scala:60 []
    |  CoGroupedRDD[3] at join at broadcastTest.scala:60 []
    +-(2) ParallelCollectionRDD[0] at parallelize at broadcastTest.scala:37 []
    +-(2) ParallelCollectionRDD[1] at parallelize at broadcastTest.scala:40 []
##################################################
*/
