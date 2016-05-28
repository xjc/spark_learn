package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object coalesceTest {

    def printParSet[T](a:RDD[T]) {
        import scala.collection.mutable.Map
            val partCheckRdd = a.mapPartitionsWithIndex((i,j) => {
            var map1 = Map[String, List[T]]()
            val index_name = "index_" + i.toString
            for (x <- j) {
                if (map1.contains(index_name)) {
                    map1(index_name) = x::map1.get(index_name).get
                } else {
                    map1.put(index_name, List(x))
                }
            }
                map1.iterator
            }
            )
            partCheckRdd.sortBy(_._1).foreach(println)

    }

    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("MapTest").setMaster("local[2]")
            val sc = new SparkContext(conf)
            val data = 1 to 10
            val a = sc.parallelize(data, 4)
            println(a.partitions)
            println(a.toDebugString)
            printParSet(a)
            

            val a1 = a.coalesce(3)
            println(a1.partitions)
            println(a1.toDebugString)
            printParSet(a1)

            val a2 = a.coalesce(5,true)
            println(a2.partitions)
            println(a2.toDebugString)
            printParSet(a2)
            //http://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce


            /**
              (index_0,List(2, 1))
              (index_1,List(5, 4, 3))
              (index_2,List(7, 6))
              (index_3,List(10, 9, 8))

             */


            sc.stop

    }
}
