package com.lei.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:59 上午 2020/4/23
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
    热门商品数据生产者测试类
 */
object KafkaProducer {

  def main(args: Array[String]): Unit = {

      writeToKafka("hotitems")

  }

  def writeToKafka(topic: String): Unit ={
    val prop = new Properties()

    val zk_servers = "node-01:9092,node-02:9092,node-03:9092"
    prop.setProperty("bootstrap.servers", zk_servers)
    prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](prop)
    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    for (line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }
}
