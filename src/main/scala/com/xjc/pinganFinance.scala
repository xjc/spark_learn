package com.xjc
import org.apache.spark.{ SparkConf, SparkContext }
import com.xjc.utils.TimeUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object pinganFinance {

  case class record(customer_phone: String, driver_id: String, third_order_id: String, status: Int)
  case class result(city_name: String, order_num: Int, cust_num: Int, driver_num: Int, fix_store_num: Int, getcar_num: Int, finish_num: Int)

  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("test").setMaster("local[10]")
    val conf = new SparkConf().setAppName("pinganFinance")
    val sc = new SparkContext(conf)
    val days1 = TimeUtils.getNdaysAgo(1)
    val days0 = TimeUtils.getNdaysAgo(0)
    val months0 = TimeUtils.getNmonthsAgo(0)
    val months1 = TimeUtils.getNmonthsAgo(1)

    val location = "hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/stg.db/t_order_roundtrip"
    //case class roundtripOrder(third_order_id:String, go_order_id:String, customer_phone:String, status:Int)
    //go_order_id, (third_order_id, customer_phone, status)
    val roundtripRdd = sc.textFile(location).
      map(_.split("\001")).
      filter(i => i(38) >= s"${days1} 07" && i(38) < s"${days0} 07").
      map(i => (i(1), (i(0), i(15), i(28))))

    val orderLocation = s"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=$months0,hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=$months1"
    //order_id,(driver_id, city_id)
    val orderRdd = sc.textFile(orderLocation).
      map(_.split("\001")).
      map(i => (i(0), (i(11), i(14)))).
      coalesce(2)

    //city_id, (customer_phone, driver_id, third_order_id, status)
    import scala.collection.mutable.{ Map, Set }
    //case class singleRecord(name:String, valueSet:Map[String, Set[String]])
    //case class singleRecord(valueSet:Map[String, Set[String]])

    val filterOrderRdd = roundtripRdd.join(orderRdd).
      map(i => (i._2._2._2, record(i._2._1._2, i._2._2._1, i._2._1._1, i._2._1._3.toInt))).
      aggregateByKey(Map[String, Set[String]]())(
        { (a: Map[String, Set[String]], b: record) =>
          {
            if (a.get("third_order_id") == None) a += ("third_order_id" -> Set(b.third_order_id))
            else a.get("third_order_id").get += b.third_order_id
            if (a.get("customer_phone") == None) a += ("customer_phone" -> Set(b.customer_phone))
            else a.get("customer_phone").get += b.customer_phone
            if (a.get("driver_id") == None) a += ("driver_id" -> Set(b.driver_id))
            else a.get("driver_id").get += b.driver_id

            if (b.status >= 9 && b.status <= 12) {
              if (a.get("fix_store") == None) a += ("fix_store" -> Set(b.third_order_id)) else a.get("fix_store").get += b.third_order_id
            }
            if (b.status >= 11 && b.status <= 12) {
              if (a.get("get_car") == None) a += ("get_car" -> Set(b.third_order_id)) else a.get("get_car").get += b.third_order_id
            }
            if (b.status == 12) {
              if (a.get("finish") == None) a += ("finish" -> Set(b.third_order_id)) else a.get("finish").get += b.third_order_id
            }
            a
          }
        }, { (x, y) =>
          {
            x ++= y
            x
          }
        }).map {
          k =>
            {
              val order_num = if (k._2.get("third_order_id") == None) 0 else k._2.get("third_order_id").get.size
              val cust_num = if (k._2.get("customer_phone") == None) 0 else k._2.get("customer_phone").get.size
              val driver_num = if (k._2.get("driver_id") == None) 0 else k._2.get("driver_id").get.size
              val fix_store_num = if (k._2.get("fix_store") == None) 0 else k._2.get("fix_store").get.size
              val getcar_num = if (k._2.get("get_car") == None) 0 else k._2.get("get_car").get.size
              val finish_num = if (k._2.get("finish") == None) 0 else k._2.get("finish").get.size
              (k._1, (order_num, cust_num, driver_num, fix_store_num, getcar_num, finish_num))
            }
        }

    val cityLocation = "hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/stg.db/t_city_config"
    val cityRdd = sc.textFile(cityLocation).map(_.split("\001")).
      map {
        case Array(a, city_id, city_name, _*) => (city_id, city_name)
      }
    cityRdd.cache
    sc.broadcast(cityRdd)

    val resultRdd = if (filterOrderRdd.count == 0) sc.parallelize(Seq(result("-", 0, 0, 0, 0, 0, 0)))
    else
      filterOrderRdd.join(cityRdd).
        //map{i=> List(i._2._2, i._2._1._1, i._2._1._2, i._2._1._3, i._2._1._4, i._2._1._5, i._2._1._6).mkString(",")
        map { i => result(i._2._2, i._2._1._1, i._2._1._2, i._2._1._3, i._2._1._4, i._2._1._5, i._2._1._6)
        }

    //import to hive table
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val resultDf = resultRdd.toDF
    resultDf.registerTempTable("result")
    hiveContext.sql(s"""insert overwrite table app.app_pingan_finance partition(dt='$days1')
                            select city_name,order_num,cust_num,driver_num,fix_store_num,getcar_num,finish_num
                              from result
                       """)

    sc.stop()
  }
}
