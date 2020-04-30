package com.lei.loginfail_detect

import java.net.URL
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:04 上午 2020/4/25
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 电商用户行为分析     恶意登录监控
 *
 * 用户在2分钟内连续2次登陆失败，判定为异常登陆
 *
 * 基本需求
 *    用户在短时间内频繁登陆失败，有程序恶意攻击的可能
 *    同一用户（可以是不同IP）在2秒内连续两次登录失败，需要报警
 *
 * 解决思路
 *    将用户的登录失败行为存入ListState，设定定时器2秒后触发，查看ListState中有几次失败登录
 *    更加精确的检测，可以使用CEP库实现事件流的模式匹配
 *
 */

// 输入的登陆事件样例类
case class LoginEvent(userId:Long, ip:String, eventType:String, eventTime:Long)
// 输出的异常报警信息样例类
case class Warning(userId:Long, firstFailTime:Long, lastFailTime:Long, warningMsg:String)

object LoginFail {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/LoginLog.csv")

    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 如果数据是乱序，则需要进行watermark处理
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    val dataStream: DataStream[Warning] = loginEventStream
      .keyBy(_.userId) // 以用户id做分组
      .process(new LoginWarning(2))


    dataStream.print()

    env.execute("login fail detect job")
  }
}

// 状态编程
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
//    val loginFailList = loginFailState.get()
//
//    // 判断类型是否fail，只添加fail的事件到状态
//    if (value.eventType == "fail"){
//      if (!loginFailList.iterator().hasNext){ // 如果之前没有数据，需要注册定时器
//        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
//      }
//
//      loginFailState.add(value)
//    } else {
//      // 如果是成功，清空状态
//      loginFailState.clear()
//    }
    /**
     因为如果一下子大量登陆失败，其实没有必要等到2秒钟后的定时器触发，故可以将定时器进行注释
     */
    if (value.eventType == "fail"){
      // 如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if (iter.hasNext){
        // 如果已经有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        if (value.eventTime < firstFail.eventTime + 2){
          // 如果两次间隔小于2秒，输出报警
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds."))
        }

        // 更新最近一次的登录失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(value)
      } else{
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    } else {
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    // 触发定时器的时候，根据状态里的失败个数决定是否输出报警
//    val allLoginFails = new ListBuffer[LoginEvent]
//    val iter: util.Iterator[LoginEvent] = loginFailState.get.iterator()
//    while (iter.hasNext) {
//      allLoginFails += iter.next()
//    }
//
//    // 判断个数
//    if (allLoginFails.length >= maxFailTimes){
//     // out.collect(Warning(ctx.getCurrentKey, ))
//      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times"))
//    }
//
//    // 报警完后，将状态清空
//    loginFailState.clear()
//
//  }
}
