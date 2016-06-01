package com.xjc.variables
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._
import org.apache.spark.sql.hive.HiveContext

/**
 *
 * Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.
 * Spark actions are executed through a set of stages, separated by distributed “shuffle” operations. Spark automatically broadcasts the common data needed by tasks within each stage. The data broadcasted this way is cached in serialized form and deserialized before running each task. This means that explicitly creating broadcast variables is only useful when tasks across multiple stages need the same data or when caching the data in deserialized form is important.
 *
 * Broadcast variables are created from a variable v by calling SparkContext.broadcast(v). The broadcast variable is a wrapper around v, and its value can be accessed by calling the value method. The code below shows this:
 * https://docs.cloud.databricks.com/docs/latest/databricks_guide/06%20Spark%20SQL%20%26%20DataFrames/05%20BroadcastHashJoin%20-%20scala.html
 * http://stackoverflow.com/questions/36849204/how-to-reference-spark-broadcast-variables-outside-of-scope
 */


object broadcastSqlTest {
  case class record(id:String, money:Int)
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("broadcastSqlTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    sc.setLogLevel("WARN")

    println(hiveContext.getConf("spark.sql.autoBroadcastJoinThreshold"))   //10485760
    hiveContext.getAllConfs.filter{_._1.toLowerCase.contains("hash")}.  foreach(println)


    val data1 = (1 to 1000000).map(i=>("a" + i -> i))
    val data2 = (for (i <- 1 to 1000000 if (i % 1000 == 1)) yield i * 2) .map(i=>("a" + i -> i))

    val df1 = sc.parallelize(data1).map{i=>record(i._1, i._2)}.toDF
    val df2 = sc.parallelize(data1).map{i=>record(i._1, i._2)}.toDF
    df1.registerTempTable("t1")
    df1.registerTempTable("t2")

    val resultDf = hiveContext.sql("""
      select t1.id, t1.money, t2.money
        from t1 
        join t2 
          on t1.id = t2.id
      """)
    println("#" * 100)
    resultDf.explain


    sc.stop
  }
}

