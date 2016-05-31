package com.xjc.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object mapPartitionsWithIndex {
  implicit def int2str(num:Int) : String = String.valueOf(num)

  def main(args:Array[String]) {
    val file = "file:///home/xjc/abc.txt"
    val conf = new SparkConf().setAppName("FlatMapTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val fileRdd = sc.textFile(file)
    val fileRdd = sc.parallelize(1 to 20, 3)

    val rdd = fileRdd.mapPartitionsWithIndex{(i, iter) => {
      var result = List[String]()
      var num = 0
      while ( iter.hasNext ) {
        num += 1
        iter.next
      }
      result.::(i + "@" + num).iterator
    }
    }

    rdd.foreach(println)


    val rdd1 = fileRdd.mapPartitionsWithIndex{(i,iter) => {
      var result = scala.collection.mutable.Map[Int,List[String]]()
      var result_list = List[String]()
      while ( iter.hasNext) {
        result_list = iter.next::result_list
      }

      result(i) = result_list
      result.iterator
    }
    }

    rdd1.collect.foreach(println)

    sc.stop
  }
}

//res8: Array[Array[String]] = Array(Array(i, always, have, some, plans), Array(when, plan, a, was, not, fit,, i, never, worried,, because, i, had, plan, b))
//res10: Array[String] = Array(i, always, have, some, plans, when, plan, a, was, not, fit,, i, never, worried,, because, i, had, plan, b)
