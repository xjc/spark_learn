package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}

object MapTest {

      def main(args:Array[String]) {
              val conf = new SparkConf().setAppName("MapTest").setMaster("local[2]")
              val sc = new SparkContext(conf)
              val data = 1 to 10
              val a = sc.parallelize(data, 2)

              //map test
              val b = a.map(_ * 2 + 1).sortBy(_ + 0)
              b.toDebugString
              b.foreach(println)

              //filter test
              val c = a.map(_ * 2 + 1).filter( _ > 8)
              c.toDebugString
              c.foreach(println)

              //flatmap test
              val data1 = List[(String,String,String)](("a","b","c"), ("d", "e", "f"), ("g", "h", "i"))
              val d = sc.parallelize(data1, 2)
              val e = d.flatMap(i=>{var a:List[String] = Nil; var b = i.productIterator;  while(b.hasNext){a=b.next.toString::a;}; a})
              //val e = d.flatMap{case(x,y,z)=>List(x,y,z)}
              e.foreach(println)

              //flatMap test
              val file = "file:///home/xjc/abc.txt"
              val fileRdd = sc.textFile(file)

              val mapRdd = fileRdd.map(_.split("\\s"))
              val flatMapRdd = fileRdd.flatMap(_.split("\\s"))
                
              mapRdd.foreach(println)
              flatMapRdd.foreach(println)

              mapRdd.collect
//res8: Array[Array[String]] = Array(Array(i, always, have, some, plans), Array(when, plan, a, was, not, fit,, i, never, worried,, because, i, had, plan, b))
              flatMapRdd.collect
//res10: Array[String] = Array(i, always, have, some, plans, when, plan, a, was, not, fit,, i, never, worried,, because, i, had, plan, b)

              sc.stop

  }
}
