package com.xjc.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 */

object rddToDataframeTest {

  // Encoders are also created for case classes.

  case class C1(name:String, id:Int)
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
    result1_b.collect.sortBy(_._1).map(i=>(i._1, i._2._1, i._2._2)).foreach(println)


    //plan a1
    ssc.sql("""select max(a1_num), max(a1_sum), max(a2_num), max(a2_sum)
                from (select sum(if(name = 'a_1', 1, 0)) as a1_num, sum(if(name = 'a_1', id, 0)) as a1_sum ,
                      sum(if(name = 'a_2', 1, 0)) as a2_num, sum(if(name = 'a_2', id, 0)) as a2_sum 
           from df1 group by name) t1 """).show

    //plan b1
    import scala.collection.mutable.{Map}
    val rdd_test = df1.map{row=>(row.getString(0), (row.getString(0), row.getInt(1)))}.
        aggregateByKey(Map[String,Int]())(
          (i,k) => {
              if (k._1 == "a_1") {
                i.put("a_1_num", i.getOrElse("a_1_num", 0) + 1)
                i.put("a_1_sum", i.getOrElse("a_1_sum", 0) + k._2 )
              }
              if (k._1 == "a_2") {
                i.put("a_2_num", i.getOrElse("a_2_num", 0) + 1)
                i.put("a_2_sum", i.getOrElse("a_2_sum", 0) + k._2 )
              }
            i
          },
          (m,n) => {
            val combResult = Map[String,Int]()
            val keySet = m.keySet ++ n.keySet
            for (k<- keySet) {
              combResult.put(k, m.getOrElse(k, 0) + n.getOrElse(k, 0) )
            }
            combResult
          }
          ).flatMap{case(i,j)=> {
            val result = Map[String,Int]()
            for (a<-j) {
              result.put(a._1, a._2)
            }
          result.iterator
          }
          }.collectAsMap
    println((rdd_test("a_1_num")  :: rdd_test("a_1_sum") :: rdd_test("a_2_num") :: rdd_test("a_2_sum") :: Nil).mkString("\t") )

    sc.stop
  }
}
