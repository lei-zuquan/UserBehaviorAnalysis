package com.lei.hotitems_analysis.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:53 上午 2020/4/20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// flink通过有状态支持，将kafka消费的offset自动进行状态保存，自动维护偏移量
object MyKafkaUtil {

  val prop = new Properties()

  val zk_servers = "node-01:9092,node-02:9092,node-03:9092"
  prop.setProperty("bootstrap.servers", zk_servers)
  prop.setProperty("group.id", "consumer_group")
  prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("auto.offset.reset", "latest")

  def getConsumer(topic:String ):FlinkKafkaConsumer[String]= {
    val myKafkaConsumer:FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    myKafkaConsumer
  }

  def getProducer(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String](zk_servers, topic, new SimpleStringSchema())
  }


}
