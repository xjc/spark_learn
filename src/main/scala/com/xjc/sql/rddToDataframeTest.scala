package com.xjc.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 */

object rddToDataframeTest {

  // Encoders are also created for case classes.

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("rddToDataframeTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame
    import ssc.implicits._

    // Define the schema using a case class.
    // // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    // // you can use custom classes that implement the Product interface.
    val rdd1 = sc.parallelize(1 to 5).flatMap(i=>(1 to i)).map(i=>("a_" + i, i))
    case class C1(name:String, id:Int)
    val df1 = rdd1.map{i=>C1(i._1, i._2)}.toDF
    df1.show
    df1.registerTempTable("df1")

    //plan a
    ssc.sql("select name, count(id), sum(id) from df1 group by name order by name").show
    //plan b
    val result1_b = df1.map{row=>(row.getString(0), row.getInt(1))}.
        aggregateByKey((0,0))(
          (i,j)=>{
            (i._1+1,i._2+j)
          },
          (m,n)=>{
            (m._1 + n._1, m._2 + n._2)
          }
          )
    result1_b.collect.sortBy(_._1).foreach(println)


    //plan a1
    ssc.sql("""select max(a1_num), max(a1_sum), max(a2_num), max(a2_sum)
                from (select sum(if(name = 'a_1', 1, 0)) as a1_num, sum(if(name = 'a_1', id, 0)) as a1_sum ,
                      sum(if(name = 'a_2', 1, 0)) as a2_num, sum(if(name = 'a_2', id, 0)) as a2_sum 
           from df1 group by name) t1 """).show

    //plan b1
    import scala.collection.mutable.{Map}
    val rdd_test = df1.map{row=>(row.getString(0), row.getInt(1))}.
        aggregateByKey(Map[String,(Int, Int)]())(
          (i,k) => {
            if (i.get(k._1) == None ) {
              i.put(k._1, (1, k._2))
            } else {
              i(k._1) = (i.get(k._1).get._1 + 1, i.get(k._1).get._2 + k._2  )
            }
            i
          },
          (m,n) => {
            val allKey = m.keySet ++ n.keySet
            val combResult = Map[String,(Int, Int)]()
            for (k<- allKey) {
              val v1 = m.getOrElse(k, (0,0))
              val v2 = n.getOrElse(k, (0,0))
              combResult.put(k, v1._1 + v2._1, v1._2 + v2._2)
            }
            combResult
          }
          )


    sc.stop
  }
}
