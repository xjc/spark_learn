package com.xjc.variables
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._
import scala.collection.mutable.Set
import  org.apache.spark.AccumulatorParam

/**
 *
 * http://stackoverflow.com/questions/26139145/spark-accumulatorparam-generic-parameters
*/


object accumulableTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("accumulableTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val accum = sc.accumulator(0, "My Accumulator")
    val rdd1 = sc.parallelize(1 to 100, 10)
    rdd1.map { x => accum += x; }
    println(accum.value)
    // Here, accum is still 0 because no actions have caused the <code>map</code> to be computed.


    val result = rdd1.foreach(x=> accum += x)
    println("result: " + accum.value)
    //5050


    val accum_set = sc.accumulator(Set[Int](), "test accumulator")(new SetAccumulatorParam[Int])
    val rdd2 = sc.parallelize(1 to 10)
    rdd2.foreach(x=> accum_set ++= Set(x))
    println("result: " + accum_set.value)
    //result: Set(9, 1, 5, 2, 6, 3, 10, 7, 4, 8)

    val accum_num = sc.accumulator(0, "test accumulator0")(new TestAccumulatorParam)
    val rdd3 = sc.parallelize(1 to 10, 1)
    rdd3.foreach(x=> {accum_num += x; println(accum_num)})
    println("result: " + accum_num.value)
    sc.stop
  }
}

class SetAccumulatorParam[T] extends AccumulatorParam[Set[T]] {

  override def zero(initialValue: Set[T]): Set[T]= {
    //Vector.zeros(initialValue.size)
    Set[T]()
  }

  //Merge two accumulated values together. Is allowed to modify and return the first value for efficiency (to avoid allocating objects).
  override def addInPlace(v1: Set[T], v2: Set[T]): Set[T]= {
    v1 ++ v2
  }
}

class TestAccumulatorParam extends AccumulatorParam[Int] {
  override def zero(initialValue:Int): Int = {
    0
  }
  override def addInPlace(v1: Int, v2:Int) : Int = {
    v1 + v2 * 2
    //v1 + v2
  }
}
