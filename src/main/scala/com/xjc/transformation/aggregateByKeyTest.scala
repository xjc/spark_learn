package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}
import scala.collection.mutable.HashSet

//http://stackoverflow.com/questions/24804619/how-does-spark-aggregate-function-aggregatebykey-work

/**
aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])

When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.

   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.

  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    // We will clean the combiner closure later in `combineByKey`
    val cleanedSeqOp = self.context.clean(seqOp)
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, partitioner)
  }

*/

object aggregateByKeyTest {

  implicit def int2str(num:Int):String = String.valueOf(num)
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("MapPartitionsTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = List(("a",1),("b",2),("a",3), ("a",4), ("b",3))
    val rdd = sc.parallelize(data, 2)

    val rdd1 = rdd.reduceByKey(_ + _)
    rdd1.collect.foreach(println)

    val rdd2 = rdd.aggregateByKey(0)(_+_, _+_)
    rdd2.collect.foreach(println)

    val rdd3 = rdd.aggregateByKey(new HashSet[Int]())(_+_, _++_)
    rdd3.collect.foreach(println)

    //test
    var data1 = sc.parallelize(List((5,3),(5,2),(5, 4),(5,3)))
    def seq(a:Int, b:Int) : Int ={
      println("seq: " + a + "\t " + b)
      math.max(a,b)
    }
    def comb(a:Int, b:Int) : Int ={
      println("comb: " + a + "\t " + b)
      a + b
    }
    data1.aggregateByKey(1)(seq, comb).collect
    data1.aggregateByKey(3)(seq, comb).collect
    data1.aggregateByKey(5)(seq, comb).collect

    sc.stop
  }
}

/**
 (b,5)
 (a,8)

 (b,5)
 (a,8)

 (b,Set(2, 3))
 (a,Set(1, 3, 4))
 */
