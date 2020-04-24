package com.lei.networkflow_analysis

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 4:58 下午 2020/4/24
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 使用布隆过滤器的 UV 统计
 *
 * 在上节的例子中，我们把所有数据的 userId 都存在了窗口计算的状态里，在窗
 * 口收集数据的过程中，状态会不断增大。一般情况下，只要不超出内存的承受范围，
 * 这种做法也没什么问题；但如果我们遇到的数据量很大呢？
 * 把所有数据暂存放到内存里，显然不是一个好注意。我们会想到，可以利用 redis
 * 这种内存级 k-v 数据库，为我们做一个缓存。但如果我们遇到的情况非常极端，数
 * 据大到惊人呢？比如上亿级的用户，要去重计算 UV。
 * 如果放到 redis 中，亿级的用户 id（每个 20 字节左右的话）可能需要几 G 甚至
 * 几十 G 的空间来存储。当然放到 redis 中，用集群进行扩展也不是不可以，但明显
 * 代价太大了。
 * 一个更好的想法是，其实我们不需要完整地存储用户 ID 的信息，只要知道他在
 * 不在就行了。所以其实我们可以进行压缩处理，用一位（bit）就可以表示一个用户
 * 的状态。这个思想的具体实现就是布隆过滤器（Bloom Filter）。
 * 本质上布隆过滤器是一种数据结构，比较巧妙的概率型数据结构（probabilistic
 * data structure），特点是高效地插入和查询，可以用来告诉你 “某样东西一定不存
 * 在或者可能存在”。
 * 它本身是一个很长的二进制向量，既然是二进制的向量，那么显而易见的，存
 * 放的不是 0，就是 1。相比于传统的 List、Set、Map 等数据结构，它更高效、占用
 * 空间更少，但是缺点是其返回的结果是概率性的，而不是确切的。
 *
 * 我们的目标就是，利用某种方法（一般是 Hash 函数）把每个数据，对应到一个
 * 位图的某一位上去；如果数据存在，那一位就是 1，不存在则为 0。
 * 接下来我们就来具体实现一下。
 * 注意这里我们用到了 redis 连接存取数据，所以需要加入 redis 客户端的依赖：
 */
/*
        <dependency>
            <groupId>redis.clients</groupId>
            <artifactId>jedis</artifactId>
            <version>2.8.1</version>
        </dependency>
 */
object UvWithBloom {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计PV操作
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

    dataStream.print()

    env.execute("uv with bloom job")
      
  }

}

// 自定义窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  // 每来一个元素，做什么操作
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  // 如果在处理语义上如何触发
  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  // 假如我们定义的时间语义是event time，我们如果触发
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }

  // 窗口关闭时，收尾工作
  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {

  }
}


// 定义一个布隆过虑器
class Bloom(size: Long) extends Serializable {
  // 位图的总大小, 16M
  private val cap = if (size > 0) size else 1 << 27

  // 定义hash函数
  def hash(value:String, seed: Int):Long = {
    var result: Long = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }

    result & (cap - 1)
  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  // 上述是每来一个元素就会触发一次，为了避免频繁创建redis连接
  // 定义redis连接
  lazy val jedis = new Jedis("localhost", 6379)
  // 64M 大小的位图，能处理5亿多的key
  lazy val bloom = new Bloom(1<< 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 位图的存储方式，key是windowEnd, value是bitmap
    val storeKey = context.window.getEnd.toString
    var count = 0L
    // 把每个窗口的uv count值也存入count的redis表，存放内容为（windowEnd -> uvCount)，所以要先从redis中读取
    if (jedis.hget("count", storeKey) != null){
      count = jedis.hget("count", storeKey).toLong
    }

    // 用布隆过虑器判断不前用户是否已经存在
    val userId = elements.last._2.toString
    val offset: Long = bloom.hash(userId, 61)
    // 定义一个标识位，判断redis位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist){
      // 如果不存在，位图对应位置1， count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}