package com.xjc
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import com.xjc.utils.udfmd5
import com.xjc.utils.TimeUtils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

import scala.collection.mutable.Set

object userValue {

  //case class singleRecord(phone :String, city_id:String, first_ord_date: String, last_ord_date:String, if_action:Int)
  case class avgPeriod(phone: String, min_created: String, max_created: String, order_range: Int, order_num: Int, order_rate: Int)
  case class OrderRangeChange(phone: String, alive_range: Int, last_order_days: Int, max_range: Int, half_max_range: Int, all_range: Int, half_all_range: Int)

  def compCreatedTime(a: String, b: String): Boolean = {
    TimeUtils.getSeconds(b) - TimeUtils.getSeconds(a) <= 43200
  }

  def minusDays(a: String, b: String): Int = {
    val time1 = if (a.length == 10) a + " 00:00:00" else a
    val time2 = if (b.length == 10) b + " 00:00:00" else b
    math.abs((TimeUtils.getSeconds(time1) - TimeUtils.getSeconds(time2)) / 86400).toInt
  }

  import scala.util.Try
  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  //获取百分之1的极值
  def getTop1PerValue(df: org.apache.spark.sql.DataFrame, colSeq: Int, allLength: Long): Double = {
    val oneperLength = (allLength / 100).toInt
    val tmp1 = df.map { df => (parseDouble(df.get(colSeq).toString).getOrElse(0.0)) }.top(oneperLength)
    tmp1(oneperLength - 1)
  }

  //连续成单最大周期
  def getMaxMonthRange(months_set: Set[String]): Int = {
    val a1 = months_set.toArray.sorted
    val length1 = a1.length
    //yyyy-mm
    def month_between(month1: String, month2: String): Int = {
      //implicit def str2int(a:String):Int = Integer.valueOf(a)
      math.abs(month2.substring(2, 4).toInt * 12 + month2.substring(5).toInt -
        month1.substring(2, 4).toInt * 12 - month1.substring(5).toInt)
    }
    var num = 0
    for (i <- 0 to (length1 - 1)) {
      for (j <- i + 1 to (length1 - 1)) {
        val num2 = month_between(a1(j), a1(i))
        num = if (num2 == j - i && num2 > num) num2 else num
      }
    }
    num
  }

  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("test").setMaster("local[10]")
    //val conf = new SparkConf().setAppName("viscosityCust").setMaster("local[4]")
    val conf = new SparkConf().setAppName("userValue")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("scrm", udfmd5.getMd5 _)

    val days30 = TimeUtils.getNdaysAgo(30)
    val days1 = TimeUtils.getNdaysAgo(1)
    val months1 = TimeUtils.getNmonthsAgo(1)
    val months0 = TimeUtils.getNmonthsAgo(0)
    val months6 = TimeUtils.getNmonthsAgo(6)
    val months12 = TimeUtils.getNmonthsAgo(12)

    import hiveContext.implicits._
    import hiveContext.sql
    //implicit def long2int(a:Long):Int = a.toInt

    val filterDf = sql("""select distinct phone 
	                        from (select phone from tmp_rjg.t_user_basic_level where user_level=10
	                               union all
			              select phone from stg.t_vip
			        ) a
                           """)
    filterDf.coalesce(2)
    //filterDf.coalesce(2).cache
    filterDf.registerTempTable("vip")

    val orderDf = sql(s"""
select a.phone, a.statday, a.income, a.order_id,
       a.created, a.status, a.driver_ready_distance, a.cost
from
(select phone, statday, if(income < 0 or income is null, 0, income) as income, 
       order_id,
       created,status, driver_ready_distance, cost
  from dwd.i_t_order
 where statmonth >= '$months12'
   and statmonth <= '$months1'
   and channel != '01005'
) a  
left outer join vip b
  on a.phone = b.phone
where b.phone is null
   """)

    orderDf.coalesce(30).cache
    orderDf.registerTempTable("order")

    val incomeKpiSql = """
select t1.phone, 
       t1.sum_income,
       t1.avg_income,
       t1.max_income,
       bonus_use_money,
       bonus_use_money * 100 / sum_income as bonus_money_rate,
       bonus_order_num * 100 / order_num as bonus_order_rate,
       order_num,
       bonus_order_num,
       cost
from(
select phone, 
       sum(a.income) as sum_income,
       avg(a.income) as avg_income,  
       max(a.income) as max_income,
       max(a.statday) as max_statday,
       min(a.statday) as min_statday,
       count(a.phone) as order_num,
       sum(if(b.order_id is not null, b.use_money, 0)) as bonus_use_money,
       sum(if(b.order_id is not null, 1, 0)) as bonus_order_num,
       sum(a.cost) as cost
  from ( select * from order where status = 1 ) a
  left outer join ( select distinct customer_phone, use_money, order_id
                      from ods.ods_t_customer_bonus_all
                     where order_id > 0
                       and used > 0
  ) b
    on a.order_id = b.order_id
 group by a.phone
) t1
	"""

    val incomeKpiDf = sql(incomeKpiSql)
    incomeKpiDf.cache
    incomeKpiDf.registerTempTable("incomeKpiDf")
    val incomeKpiDfLength = incomeKpiDf.count
    val sum_income_1 = getTop1PerValue(incomeKpiDf, 1, incomeKpiDfLength)
    val avg_income_1 = getTop1PerValue(incomeKpiDf, 2, incomeKpiDfLength)
    val max_income_1 = getTop1PerValue(incomeKpiDf, 3, incomeKpiDfLength)

    val costEffect = incomeKpiDf.map {
      row => row.getDouble(9)
    }.
      filter(_ > 0)
    val costEffectLength = costEffect.count.toInt
    println("#" * 50)
    println(costEffectLength)
    println("#" * 50)
    val cost_tmp_1 = costEffect.top((costEffectLength / 100).toInt)
    val cost_1 = cost_tmp_1((costEffectLength / 100).toInt - 1)

    //12小时只取一单  
    //phone, min_created, max_created, order_range, order_num, order_rate)
    val oneOrder12hoursRdd = orderDf.
      filter(orderDf("status") === 1).
      rdd.map {
        row => (row.getString(0), row.getString(4))
      }.groupByKey.map(i => {
        val allCreated = i._2.toBuffer
        val sortedallCreated = allCreated.sorted
        var time1 = "1970-01-01 08:00:00"
        for (i <- sortedallCreated) {
          if (compCreatedTime(time1, i)) {
            allCreated -= i
          }
          time1 = i
        }
        (i._1, (allCreated.max, allCreated.min, allCreated.size))
      }).map { i =>
        (i._1, i._2._2, i._2._1, minusDays(i._2._2, i._2._1), i._2._3, if (i._2._3 == 0) 0 else minusDays(i._2._2, i._2._1) / i._2._3)
      }.map(i => avgPeriod(i._1, i._2, i._3, i._4, i._5, i._6))
    val oneOrder12hoursDf = oneOrder12hoursRdd.toDF

    val halfYearoneOrder12hoursRdd = orderDf.
      filter(orderDf("status") === 1 && orderDf("created") > months6).
      rdd.map {
        row => (row.getString(0), row.getString(4))
      }.groupByKey.map(i => {
        val allCreated = i._2.toBuffer
        val sortedallCreated = allCreated.sorted
        var time1 = "1970-01-01 08:00:00"
        for (i <- sortedallCreated) {
          if (compCreatedTime(time1, i)) {
            allCreated -= i
          }
          time1 = i
        }
        (i._1, (allCreated.max, allCreated.min, allCreated.size))
      }).map { i =>
        (i._1, i._2._2, i._2._1, minusDays(i._2._2, i._2._1), i._2._3, if (i._2._3 <= 1) 0 else minusDays(i._2._2, i._2._1) / (i._2._3 - 1))
      }.map(i => avgPeriod(i._1, i._2, i._3, i._4, i._5, i._6))
    //成单周期对比,一年对比最近半年
    val order_rate_trend_tmp_rdd = oneOrder12hoursRdd.map { case avgPeriod(phone, a, b, c, d, order_rate) => (phone, order_rate) }.
      join(halfYearoneOrder12hoursRdd.map { case avgPeriod(phone, a, b, c, d, order_rate) => (phone, order_rate) }).
      map(i => (i._1, if (i._2._2 == 0) 0 else i._2._1 / i._2._2)).
      filter(_._2 > 0)
    val order_rate_trend_df = order_rate_trend_tmp_rdd.toDF("phone", "order_rate_change")
    order_rate_trend_df.registerTempTable("order_rate_trend_df")
    val order_rate_trend_tmp_rdd_1 = order_rate_trend_tmp_rdd.map {
      i => i._2
    }
    val order_rate_trend_tmp_length = order_rate_trend_tmp_rdd_1.count.toInt
    val order_rate_trend_tmp_1 = order_rate_trend_tmp_rdd_1.top((order_rate_trend_tmp_length / 100).toInt)
    val order_rate_trend_1 = order_rate_trend_tmp_1((order_rate_trend_tmp_length / 100).toInt - 1)

    val order_rate_tmp = oneOrder12hoursRdd.map {
      case avgPeriod(a, b, c, d, e, f) => f
    }.
      filter(_ > 0).
      map(1 / _)
    val order_rate_tmp_length = order_rate_tmp.count.toInt
    val order_rate_tmp_1 = order_rate_tmp.top((order_rate_tmp_length / 100).toInt)
    val order_rate_1 = order_rate_tmp_1((order_rate_tmp_length / 100).toInt - 1)
    println("!" * 50)
    println(order_rate_1)
    println("!" * 50)

    //一年内充值金额
    val chargeDf = sql(s"""
select b.phone, cast(a.year_amount as double) as year_amount, a.half_year_amount
from
(
select user_id, 
       sum(if(create_time >= '', coalesce(amount,0), 0)) as year_amount,
       sum(if(create_time > '$months6', coalesce(amount,0), 0)) as half_year_amount
  from stg.t_customer_trans
 where ((trans_type = 28 and source = 28)
    or (trans_type = 29 and source = 29 )
    or (trans_type = 2)
    or (trans_type = 10 and source = 5)
    or (trans_type = 65 and source = 65)
    or (trans_type = 66 and source = 66))
   and create_time >= '$months12' and create_time < '$months0'
 group by user_id
) a
  join stg.t_customer_main b
    on a.user_id = b.id
       """)
    val year_amount_tmp_rdd = chargeDf.map {
      row => row.getDouble(2)
    }.
      filter(_ > 0)
    val year_amount_tmp_length = year_amount_tmp_rdd.count.toInt
    val year_amount_tmp_1 = year_amount_tmp_rdd.top((year_amount_tmp_length / 100).toInt)
    val year_amount_1 = year_amount_tmp_1((year_amount_tmp_length / 100).toInt - 1)

    //间接价值
    val indirectValueDf = sql("""
select a.phone, 
       a.user_cancel_num * 100 / order_num as user_cancel_rate,
       a.driver_ready_cancel_num * 100 / order_num as driver_ready_cancel_rate,
       coalesce(b.complain_num, 0) * 100 / order_num as complain_rate,
       coalesce(c.comment_num,0) * 100 / order_num as comment_rate,
       a.user_cancel_num, a.driver_ready_cancel_num, complain_num, comment_num, a.order_num
from
(
select phone,
       sum(if(status = 6, 1, 0)) as user_cancel_num,
       sum(if(status=6 and driver_ready_distance>0, 1, 0)) as driver_ready_cancel_num,
       count(1) as order_num
  from order
 group by phone
) a
left outer join ( select driver_phone, count(1) as complain_num
                    from stg.t_customer_complain
                   where source = 8
                   group by driver_phone
) b
  on a.phone = b.driver_phone
left outer join ( select sender, count(1) as comment_num from stg.t_comment_sms group by sender
) c
  on a.phone = c.sender
       """)

    indirectValueDf.cache
    indirectValueDf.registerTempTable("indirectValueDf")
    //indirect top values
    //user_cancel_rate 负向指标
    val user_cancel_rate_tmp_rdd = indirectValueDf.map {
      row => row.getDouble(1)
    }.
      filter(_ > 0).
      map(1 / _)
    val user_cancel_rate_tmp_length = user_cancel_rate_tmp_rdd.count.toInt
    val user_cancel_rate_tmp_1 = user_cancel_rate_tmp_rdd.top((user_cancel_rate_tmp_length / 100).toInt)
    val user_cancel_rate_1 = user_cancel_rate_tmp_1((user_cancel_rate_tmp_length / 100).toInt - 1)

    //司机就位后消单率
    val rate_tmp_rdd = indirectValueDf.map {
      row => row.getDouble(2)
    }.
      filter(_ > 0)
    val rate_tmp_length = rate_tmp_rdd.count.toInt
    val rate_tmp_1 = rate_tmp_rdd.top((rate_tmp_length / 100).toInt)
    val driver_ready_cancel_rate_1 = rate_tmp_1((rate_tmp_length / 100).toInt - 1)

    //回评率
    val comment_rate_tmp_rdd = indirectValueDf.map {
      row => row.getDouble(4)
    }.
      filter(_ > 0)
    val comment_rate_tmp_length = comment_rate_tmp_rdd.count.toInt
    val comment_rate_tmp_1 = comment_rate_tmp_rdd.top((comment_rate_tmp_length / 100).toInt)
    val comment_rate_1 = comment_rate_tmp_1((comment_rate_tmp_length / 100).toInt - 1)

    //order_num
    val order_num_tmp_rdd = indirectValueDf.map {
      row => row.getLong(9).toInt
    }.
      filter(_ > 0)
    val order_num_tmp_length = order_num_tmp_rdd.count.toInt
    val order_num_tmp_1 = order_num_tmp_rdd.top((order_num_tmp_length / 100).toInt)
    val order_num_1 = order_num_tmp_1((order_num_tmp_length / 100).toInt - 1)

    incomeKpiDf.registerTempTable("incomeKpiDf")
    oneOrder12hoursDf.registerTempTable("oneOrder12hoursDf")
    chargeDf.registerTempTable("chargeDf")

    val user_value_part1_df = sql(s"""
select a.phone,
       coalesce(if(sum_income >= $sum_income_1, 100, sum_income * 100 / $sum_income_1) * 0.192, 0)  as sum_income_rate,
       coalesce(if(avg_income >= $avg_income_1, 100, avg_income * 100 / $avg_income_1) * 0.12, 0)  as avg_income_rate,
       coalesce(if(max_income >= $max_income_1, 100, max_income * 100 / $avg_income_1) * 0.096, 0) as max_income_rate,
       (case
         when b.order_rate = 0 then 0
         when 1/b.order_rate >= $order_rate_1 then 100
         else $order_rate_1 / b.order_rate * 100
       end) * 0.072 as order_rate,
       coalesce(if(c.year_amount >= $year_amount_1, 100, c.year_amount * 100 / $year_amount_1) * 0.096, 0) as year_amount_rate,
       coalesce(if(a.cost >= $cost_1, 100, a.cost / $cost_1 * 100) * 0.224, 0) as cost_rate
  from incomeKpiDf a 
  left outer join oneOrder12hoursDf b
    on a.phone = b.phone
  left outer join chargeDf c
    on a.phone = c.phone
        """)
    val user_value_part2_df = sql(s"""
select phone,
       coalesce(if(user_cancel_rate = 0, 100, if(1/user_cancel_rate >= $user_cancel_rate_1, 100, 1 / $user_cancel_rate_1 / user_cancel_rate * 100)) * 0.03, 0) as user_cancel_rate,
       coalesce(if(driver_ready_cancel_rate = 0, 100, if(1/driver_ready_cancel_rate >= $driver_ready_cancel_rate_1, 100, 1 / $driver_ready_cancel_rate_1 / driver_ready_cancel_rate * 100)) * 0.07, 0) as driver_ready_cancel_rate,
       coalesce(if(comment_rate >= $comment_rate_1, 100, comment_rate * 100 / $comment_rate_1) * 0.05, 0) as comment_rate,
       coalesce(if(order_num >= $order_num_1, 100, order_num * 100 / $order_num_1) * 0.05, 0) as order_num
  from indirectValueDf
       """)

    user_value_part1_df.registerTempTable("user_value_part1_df")
    user_value_part2_df.registerTempTable("user_value_part2_df")

    val result = sql(s"""
insert overwrite table app.app_mkt_user_value_sum_ma_v2 partition(dmon='$months1')
select scrm(t1.phone, 'EDJ-DW-1024') as user_id,

       coalesce(round(sum_income_rate , 2), 0) +  coalesce(round(avg_income_rate , 2), 0) +  coalesce(round(max_income_rate , 2), 0) + 
       coalesce(round(order_rate , 2), 0) +  coalesce(round(year_amount_rate , 2), 0) +  coalesce(round(cost_rate , 2), 0) + 
       coalesce(round(user_cancel_rate , 2), 0) +  coalesce(round(driver_ready_cancel_rate , 2), 0) +  
       coalesce(round(comment_rate , 2), 0) +  coalesce(round(order_num , 2), 0) as score,
       coalesce(round(sum_income_rate          , 2), 0),
       coalesce(round(avg_income_rate          , 2), 0),
       coalesce(round(max_income_rate          , 2), 0),
       coalesce(round(order_rate               , 2), 0),
       coalesce(round(year_amount_rate         , 2), 0),
       coalesce(round(cost_rate                , 2), 0),
       coalesce(round(user_cancel_rate         , 2), 0),
       coalesce(round(driver_ready_cancel_rate , 2), 0),
       coalesce(round(comment_rate             , 2), 0),
       coalesce(round(order_num                , 2), 0),
       t2.real_or_regeister_city_id as city_id
from
(
select coalesce(a.phone, b.phone) as phone,
       a.sum_income_rate,
       a.avg_income_rate,
       a.max_income_rate,
       a.order_rate,
       a.year_amount_rate,
       a.cost_rate,
       b.user_cancel_rate,
       b.driver_ready_cancel_rate,
       b.comment_rate,
       b.order_num
  from user_value_part1_df a
  full outer join user_value_part2_df b 
    on a.phone = b.phone
) t1
left outer join dwa.app_all_phone_city t2
  on t1.phone = t2.phone
    """)

    //user_loyalty_value
    case class c1(max_day: String, min_day: String, month_set: scala.collection.mutable.Set[String])
    //case class OrderRangeChange(phone:String, max_day:String, min_day:String, max_range:Int, half_max_range:Int, all_range:Int, half_all_range:Int)
    val orderRdd = sql("select phone, statday  from order where status = 1")
    val orderRangeChangeDf = orderRdd.map(row => (row.getString(0), row.getString(1))).
      aggregateByKey(c1("1970-01-01", "9999-12-31", Set()))(
        {
          case (x: c1, y: String) => {
            val max_day = if (x.max_day >= y) x.max_day else y
            val min_day = if (x.min_day >= y) y else x.min_day
            c1(max_day, min_day, x.month_set + y.substring(0, 7))
          }
        },
        {
          case (x: c1, y: c1) => {
            val max_day = if (x.max_day >= y.max_day) x.max_day else y.max_day
            val min_day = if (x.min_day >= y.min_day) y.min_day else x.min_day
            c1(max_day, min_day, x.month_set ++: y.month_set)
          }
        }).map {
          case (phone, c1(max_day, min_day, months_set)) => {
            val max_range = getMaxMonthRange(months_set)
            val alive_range = minusDays(max_day, min_day)
            val last_order_days = minusDays(max_day, days1)
            val half_months_set = months_set.filter(_ > months6)
            val half_max_range = getMaxMonthRange(half_months_set)
            OrderRangeChange(phone, alive_range, last_order_days, max_range, half_max_range, months_set.size, half_months_set.size)
          }
        }.toDF
    orderRangeChangeDf.registerTempTable("orderRangeChangeDf")
    val user_loyalty_value_df = sql("""
select t1.phone,
       t6.real_or_regeister_city_id as city_id,
       t1.alive_range,
       t1.max_range,
       t1.all_range,
       t1.last_order_days,
       if(t1.last_order_days != 0, t4.order_rate * 100 / t1.last_order_days, 0) as last_order_avg_comp,
       coalesce(if(t2.order_num <= 2 , 0, t3.order_rate_change), 0) as order_rate_change,
       coalesce(t2.order_num, 0) as order_num,
       coalesce(t5.year_amount, 0) as year_amount
  from orderRangeChangeDf t1
  left outer join ( select phone, count(1) as order_num
                      from order
                     where status = 1
                    group by phone
   ) t2
    on t1.phone = t2.phone
  left outer join order_rate_trend_df t3
    on t1.phone = t3.phone
  left outer join oneOrder12hoursDf t4
    on t1.phone = t4.phone
  left outer join chargeDf t5
    on t1.phone = t5.phone
  left outer join dwa.app_all_phone_city t6
    on t1.phone = t6.phone
""")
    user_loyalty_value_df.cache
    user_loyalty_value_df.registerTempTable("user_loyalty_value_df")
    //top 1 percent value
    val alive_range_tmp_rdd = user_loyalty_value_df.map {
      row => row.getInt(2)
    }.
      filter(_ > 0)
    val alive_range_tmp_length = alive_range_tmp_rdd.count.toInt
    val alive_range_tmp_1 = alive_range_tmp_rdd.top((alive_range_tmp_length / 100).toInt)
    val alive_range_1 = alive_range_tmp_1((alive_range_tmp_length / 100).toInt - 1)

    val max_range_tmp_rdd = user_loyalty_value_df.map {
      row => row.getInt(3)
    }.
      filter(_ > 0)
    val max_range_tmp_length = max_range_tmp_rdd.count.toInt
    val max_range_tmp_1 = max_range_tmp_rdd.top((max_range_tmp_length / 100).toInt)
    val max_range_1 = max_range_tmp_1((max_range_tmp_length / 100).toInt - 1)

    val all_range_tmp_rdd = user_loyalty_value_df.map {
      row => row.getInt(4)
    }.
      filter(_ > 0)
    val all_range_tmp_length = all_range_tmp_rdd.count.toInt
    val all_range_tmp_1 = all_range_tmp_rdd.top((all_range_tmp_length / 100).toInt)
    val all_range_1 = all_range_tmp_1((all_range_tmp_length / 100).toInt - 1)

    val last_order_days_tmp_rdd = user_loyalty_value_df.map {
      row => row.getInt(5)
    }.
      filter(_ > 0)
    val last_order_days_tmp_length = last_order_days_tmp_rdd.count.toInt
    val last_order_days_tmp_1 = last_order_days_tmp_rdd.top((last_order_days_tmp_length / 100).toInt)
    val last_order_days_1 = last_order_days_tmp_1((last_order_days_tmp_length / 100).toInt - 1)

    val last_order_avg_comp_tmp_rdd = user_loyalty_value_df.map {
      row => row.getDouble(6)
    }.
      filter(_ > 0)
    val last_order_avg_comp_tmp_length = last_order_avg_comp_tmp_rdd.count.toInt
    val last_order_avg_comp_tmp_1 = last_order_avg_comp_tmp_rdd.top((last_order_avg_comp_tmp_length / 100).toInt)
    val last_order_avg_comp_1 = last_order_avg_comp_tmp_1((last_order_avg_comp_tmp_length / 100).toInt - 1)

    val order_rate_change_comp_tmp_rdd = user_loyalty_value_df.map {
      row => row.getInt(7)
    }.
      filter(_ > 0)
    val order_rate_change_comp_tmp_length = order_rate_change_comp_tmp_rdd.count.toInt
    val order_rate_change_comp_tmp_1 = order_rate_change_comp_tmp_rdd.top((order_rate_change_comp_tmp_length / 100).toInt)
    val order_rate_change_comp_1 = order_rate_change_comp_tmp_1((order_rate_change_comp_tmp_length / 100).toInt - 1)

    val finished_order_num_tmp_rdd = user_loyalty_value_df.map {
      row => row.getLong(8).toInt
    }.
      filter(_ > 0)
    val finished_order_num_tmp_length = finished_order_num_tmp_rdd.count.toInt
    val finished_order_num_tmp_1 = finished_order_num_tmp_rdd.top((finished_order_num_tmp_length / 100).toInt)
    val finished_order_num_1 = finished_order_num_tmp_1((finished_order_num_tmp_length / 100).toInt - 1)

    val year_amount_comp_tmp_rdd = user_loyalty_value_df.map {
      row => row.getDouble(9)
    }.
      filter(_ > 0)
    val year_amount_comp_tmp_length = year_amount_comp_tmp_rdd.count.toInt
    val year_amount_comp_tmp_1 = year_amount_comp_tmp_rdd.top((year_amount_comp_tmp_length / 100).toInt)
    val year_amount_comp_1 = year_amount_comp_tmp_1((year_amount_comp_tmp_length / 100).toInt - 1)

    val update_loyalty_data = s"""
insert overwrite table app.app_mkt_user_loyalty_sum_ma_v2 partition(dmon='$months1')
select phone,
       alive_range + max_range + all_range + last_order_days + last_order_avg_comp + 
       order_rate_change + order_num + charge_or_not_rate + charge_rate as score,
       alive_range           ,                             
       max_range             ,                             
       all_range             ,                             
       last_order_days       ,                             
       last_order_avg_comp   ,                             
       order_rate_change     ,                             
       order_num             ,                             
       charge_or_not_rate    ,                             
       charge_rate           ,
       city_id
  from (
select phone, city_id, 
       coalesce(round(if(t1.alive_range >= ${alive_range_1}, 100, t1.alive_range * 100 / $alive_range_1) * 0.08, 2), 0) as alive_range,
       coalesce(round(if(t1.max_range >= ${max_range_1}, 100, t1.max_range * 100 / $max_range_1) * 0.16, 2), 0) as max_range,
       coalesce(round(if(t1.all_range >= ${all_range_1}, 100, t1.all_range * 100 / $all_range_1) * 0.16, 2), 0) as all_range,
       coalesce(round(if(t1.last_order_days is null, 0, if(t1.last_order_days <= $last_order_days_1, 100, $last_order_days_1 * 100 / t1.last_order_days ) * 0.09), 2), 0) as last_order_days,
       coalesce(round(if((last_order_avg_comp >= ${last_order_avg_comp_1}), 100, last_order_avg_comp / $last_order_avg_comp_1) * 0.12, 2), 0) as last_order_avg_comp,
       coalesce(round(if(order_rate_change >= ${order_rate_change_comp_1}, 100, 
	  order_rate_change / ${order_rate_change_comp_1} ) * 0.18, 2), 0) as order_rate_change,
       coalesce(round(if(order_num >= ${finished_order_num_1}, 100, order_num * 100 / ${finished_order_num_1}) * 0.06, 2), 0) as order_num,
       coalesce(if(year_amount > 0, 100, 0) * 0.09, 0) as charge_or_not_rate,
       coalesce(round(if(year_amount >= ${year_amount_1}, 100, year_amount * 100 / ${year_amount_1} ) * 0.06, 2), 0) as charge_rate
  from user_loyalty_value_df t1
) t2
"""

    sql(update_loyalty_data)

    sc.stop()
  }
}
