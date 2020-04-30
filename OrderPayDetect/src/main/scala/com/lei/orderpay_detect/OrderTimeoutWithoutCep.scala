package com.lei.orderpay_detect

import java.net.URL

import com.lei.orderpay_detect.OrderTimeout.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:19 下午 2020/4/25
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 订单支付实时监控 使用 Process Function 实现
 *
 *
 */

object OrderTimeoutWithoutCep {

  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取订单数据
    val resource: URL = getClass.getResource("/OrderLog.csv")

    val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile(resource.getPath)
    //val orderEventStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 因为数据事件时间是有序的
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 定义process function 进行超时检测，但有很多问题比如延时性要15分钟
    // val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())
    // timeoutWarningStream.print()


    // 此方式，实时性非常好，代价就是业务逻辑代码很多
    val orderResultStream = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout without cep job")
      
  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
    // 保存pay 是否来过的状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))
    // 保存定时器的时间戳为状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))


    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 先读取状态
      val isPayed: Boolean = isPayedState.value()
      val timerTs: Long = timerState.value()

      // 根据事件的类型进行分类判断，做不同的处理逻辑
      if (value.eventType == "create"){
        // 如果是create事件，接下来判断pay是否来过
        if (isPayed){
          // 1.1 如果已经pay过，匹配成功，输出注流，清空状态
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 1.2 如果没有pay过，注册定时器等待pay的到来
          val ts: Long = value.eventTime * 1000L + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay"){
        // 2. 如果是pay事件，那么判断是否create过，用timer表示
        if (timerTs > 0) {
          // 2.1 如果有定时器，说明已经有create来过
          // 继续判断，是否超过了timeout时间
          if (timerTs > value.eventTime * 1000L){
            // 2.1.1 如果定时器时间还没到，那么输出成功匹配
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            // 2.1.2 如果当前pay的时间已经超时，那么输出到侧输出流
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          // 输出结束，清空状态
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()

        } else {
          // 2.2 pay先到了，更新状态，注册定时器，等待create
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timerState.update(value.eventTime * 1000L)
        }
      }
    }

    // 定时器：一种是create来了，pay没有触发定时器； 另一种是pay先来了，create还没来触发定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 根据状态的值，判断哪个数据没业
      if (isPayedState.value()) {
        // 如果为true，表示pay先到了，没等到create
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
      } else {
        // 表示create 到了，没等到pay
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timerState.clear()
    }
  }
}




// 实现自定义处理函数
class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 保存pay 是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    // 先取出状态标识位
    val isPayed = isPayedState.value()

    if (value.eventType == "create" && !isPayed){
      // 如果遇到了create事件，并且pay没有来过，注册定时器开始等待
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15*60*1000L)
    } else if (value.eventType == "pay") {
      // 如果是pay事件，直接把状态改为true
      isPayedState.update(true)

    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 判断isPayed是否为true
    val isPayed: Boolean = isPayedState.value()
    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    // 清空状态
    isPayedState.clear()
  }
}
