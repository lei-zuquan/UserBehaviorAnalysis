package com.lei.networkflow_analysis

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:55 下午 2020/4/24
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 网站独立访问数（uv) 的统计
 *    在上节的例子中，我们统计的是所有用户对页面的所有浏览行为，也就是说，
 * 同一用户的浏览行为会被重复统计。而在实际应用中，我们往往还会关注，在一段
 * 时间内到底有多少不同的用户访问了网站。
 * 另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）。UV
 * 指的是一段时间（比如一小时）内访问网站的总人数，1 天内同一访客的多次访问
 * 只记录为一个访客。通过 IP 和 cookie 一般是判断 UV 值的两种方式。当客户端第一
 * 次访问某个网站服务器的时候，网站服务器会给这个客户端的电脑发出一个 Cookie，
 * 通常放在这个客户端电脑的 C 盘当中。在这个 Cookie 中会分配一个独一无二的编号，
 * 这其中会记录一些访问服务器的信息，如访问时间，访问了哪些页面等等。当你下
 * 次再访问这个服务器的时候，服务器就可以直接从你的电脑中找到上一次放进去的
 * Cookie 文件，并且对其进行一些更新，但那个独一无二的编号是不会变的。
 * 当然，对于 UserBehavior 数据源来说，我们直接可以根据 userId 来区分不同的
 * 用户。
 */
case class UvCount(windowEnd:Long, uvCount:Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[UvCount] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计PV操作
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print()

    env.execute("uv job")
  }

}


class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据 userId并去重
    var idSet: Set[Long] = Set[Long]()
    // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for(userBehavior <- input){
      idSet += userBehavior.userId
    }

    out.collect(UvCount(window.getEnd, idSet.size))

    // 这里有个严重的问题，如果数据量非常大，会内存吃紧，甚至OOM

    // 如果内存不够，可以放到redis， 或者布隆过虑器

    // 为了极大的节省空间。数据压缩，数据存不存在，bool -> 位 -> 位图 -> 布隆过虑器

  }
}