import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext



object test1 extends App {
val conf = new SparkConf().setAppName("aa").setMaster("local[2]")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
val hiveContext = new HiveContext(sc)
import sqlContext.implicits._
val a = hiveContext.sql("show databases")
a.foreach(println)

sc.stop
	
}
