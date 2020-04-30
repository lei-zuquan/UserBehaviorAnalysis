package com.lei.loginfail_detect

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 12:13 下午 2020/4/25
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
 *
 * 电商用户行为分析     恶意登录监控 CEP实现
 *
 */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/LoginLog.csv")

    // 1. 读取事件数据，创建简单事件流
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 如果数据是乱序，则需要进行watermark处理
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)

    // 2. 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail") // 严格
      .within(Time.seconds(2))

    // 3. 在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    // 4. 从patterStream 上应用select function，检出匹配事件序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()

    env.execute("login fail with cep job")
      
  }

}


class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning]{
  // 检测到所有事件，存放到map里
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中按照名称取出对应的事件
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!")

  }
}