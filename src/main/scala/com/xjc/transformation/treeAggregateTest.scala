package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * def treeAggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U, depth: Int = 2)(implicit arg0: ClassTag[U]): U
 * Aggregates the elements of this RDD in a multi-level tree pattern.
 * depth
 * suggested depth of the tree (default: 2)
 * http://stackoverflow.com/questions/29860635/how-to-interpret-rdd-treeaggregate/29865686#29865686
 * treeAggregate is a specialized implementation of aggregate that iteratively applies the combine function to a subset of partitions. This is done in order to prevent returning all partial results to the driver where a single pass reduce would take place as the classic aggregate does.
 *
 */

object treeAggregateTest {

    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("treeAggregateTest").setMaster("local[2]")
            val sc = new SparkContext(conf)
            sc.setLogLevel("ERROR")
            val data = sc.parallelize(1 to 10,3)
            data.treeAggregate(0)(_ + _, _ + _)
            data.foreach(println)

            data.treeAggregate(1)((a:Int,b:Int) => {
                    var c = a+b
                    println(s"combine: $c")
                    c
                    },{(a:Int,b:Int) => {
                    var c = a+b
                    println(s"merge: $c")
                    c
                    }
                    }
                    )
            //59

            import scala.collection.mutable.{Map,Set}
        var rdd1 = sc.parallelize(List("a"->1, "b"->2, "c"->3, "d"->4, "a"->2, "b"->5, "d"->6), 3)
            val rdd1_1 = rdd1.treeAggregate(Map[String,Set[Int]]())(
                    (a, b) => { 
                    if (a.get(b._1) != None) 
                    a.get(b._1).get += b._2
                    else
                    a.put(b._1, Set(b._2))
                    a
                    },
                    (a, b) => {
                    var key_set = b.keySet ++ a.keySet
                    var result = Map[String,Set[Int]]()
                    for (i <- key_set) {
                    result.put(i, a.getOrElse(i, Set()) ++ b.getOrElse(i, Set()))
                    }
                    result
                    }
                    )

            rdd1_1.foreach(println)
            //res58: scala.collection.mutable.Map[String,scala.collection.mutable.Set[Int]] = Map(b -> Set(5, 2), d -> Set(6, 4), a -> Set(1, 2), c -> Set(3))

            sc.stop

    }
}
