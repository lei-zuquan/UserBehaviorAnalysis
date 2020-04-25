package com.lei.orderpay_detect

import java.net.URL
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:33 下午 2020/4/25
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 订单支付实时监控 CEP实现
 *
 */
// 定义输入订单事件的样例类
case class OrderEvent(orderId:Long, eventType:String, txId: String, eventTime:Long)
// 定义输出结果样例类
case class OrderResult(orderId:Long, resultMsg:String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取订单数据
    val resource: URL = getClass.getResource("/OrderLog.csv")

    //val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile(resource.getPath)
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 因为数据事件时间是有序的
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 2. 定义一个匹配模式: 同一个订单，先创建，接着支付，再加约束条件
    //    我们最关心的是订单创建有开头，没有支付结尾的数据
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3. 把模式应用到stream上，得到一个pattern stream
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    // 4. 调用select方法，提取事件序列，超时的事件做报警提示
    val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream: DataStream[OrderResult] = patternStream.select(orderTimeOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())


    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeOutputTag).print("timeout")

    env.execute("order time job")
  }
}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    // 有开头，没有结尾的序列都存放在map里
    val timeoutOrderId: Long = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}


// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId: Long = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}