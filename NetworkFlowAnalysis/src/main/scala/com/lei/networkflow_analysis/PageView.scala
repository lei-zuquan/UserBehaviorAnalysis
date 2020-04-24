package com.lei.networkflow_analysis

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:30 下午 2020/4/24
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 网站总浏览量（PV）的统计
 *
 * 基于埋点日志数据的网络流量统计:
 *    但有一个问题就是同一个用户可能频繁点击网页
 *
 *    我们发现，从web服务器log中得到的 url，往往更多的是请求某个资源地址（*.js、*.css），如果
 * 要针对页面进行统计往往还需要进行过滤。而在实际电商应用中，相比每个单独页面的访问量，我们可能更加
 * 关心整个电商网站的网络流量。这个指标，除了合并之前每个页面的统计结果之外，还可以通过统计埋点日志
 * 数据中的"PV" 行为来得到。
 *
 * 我们知道，用户浏览页面时，会从浏览器向网络服务器发出一个请求（Request)，网络服务器接到这个请求后，
 * 会将该请求对应的一个网页（Page) 发送给浏览器，
 * 从而产生了一个PV。所以我们的统计方法，可以是从web 服务器的日志中去提取对应的页面访问然后统计，就
 * 向上一节中的做法一样；也可以直接从埋点日志中提取用户发来的页面请求，从而统计出总浏览量。
 *    所以，接下来我们用 UserBehavior.csv 作为数据源，实现一个网站总浏览量的统计。我们可以设置滚动
 * 时间窗口，实时统计每小时内的网站PV.
 *
 */

// 定义输出数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp:Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[(String, Int)] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计PV操作
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count:")

    env.execute("page view job")
  }

}
