package com.edj.com.edj.kafka

import java.util.HashMap
import com.edj.parser.LogParser
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by xjc on 16-4-5.
 */
object MessConsumer {
  def main(args: Array[String]): Unit = {
    val zkQuorum = "localhost:2181"
    val group = "group1"

    val sparkConf = new SparkConf(true).setAppName("consumer_kafka").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topicsMap = Map("nginx_api_log" -> 2)
    val storageLevel = StorageLevel.MEMORY_ONLY
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicsMap, storageLevel)

    //write the messate to kafka
    val brokers = "localhost:9092"
    val props = new HashMap[String, Object]()
    val outTopics = "nginx_api_log_wash2"
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    lines.foreachRDD(rdd => {
      rdd.foreachPartition(record => {
        val producer = new KafkaProducer[String, String](props)
        for (i <- record) {
          println(i)
          val formatLog = LogParser.parseLog(i._2)
          formatLog match {
            case None => None
            case Some(x) => {
              producer.send(new ProducerRecord[String, String](outTopics, null, x))
            }
          }

        }
        producer.close()
      })
    }

    )

    ssc.start()
    ssc.awaitTermination()

  }


}
