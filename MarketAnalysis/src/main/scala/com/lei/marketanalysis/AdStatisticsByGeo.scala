package com.lei.marketanalysis

import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:56 上午 2020/4/25
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
/**
 * 电商用户行为分析   页面广告点击量统计，黑名单过滤
 *
 * 基本需求：
 *    从埋点日志中，统计每小时页面广告的点击量，5秒刷新一次，并按照不同省份进行划分
 *    对于 "刷单" 式频繁点击行为行为过虑，并将该用户加入黑名单
 *
 * 解决思路：
 *    根据省份进行分组，创建长度为1小时、滑动距离为5秒的时间窗口进行统计
 *    可以用process function进行黑名单过虑，检测用户对同一广告的点击量，
 *    如果超过上限则将用户信息以侧输出流输出到黑名单中
 */

// 输入的广告点击事件样例类
case class AdClickEvent(userId: Long, adId:Long, provice:String, city:String, timestamp: Long)
// 按照省份统计的输出结果样例类
case class CountByProvince(windowEnd:String, province:String, count:Long)
// 输出的黑名单报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {
  // 定义一个侧输出流的TAG标签
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource: URL = getClass.getResource("/AdClickLog.csv")
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      // 指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 自定义process function，过虑大量刷点击的行为
    val filterBlackListStream: DataStream[AdClickEvent] = adEventStream.keyBy(data => (data.userId, data.adId))
      .process(new FilterBlockProcess(100)) // process 相当于复杂版的filter


    // 根据省份做分组，开窗聚合
    val dataStream: DataStream[CountByProvince] = filterBlackListStream
      .keyBy(_.provice)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    dataStream.print("ad count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")


    env.execute("ad statistics job")
  }

  class FilterBlockProcess(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 保存是否发送黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))


    // processElement表示每来一条数据，做什么操作
    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      // 来了一个广告，判断用户点击是否超过点击量
      // 如果黑名单已经发送，不需要再发送

      // 取出count状态
      val curCount: Long = countState.value()

      // 如果是第一次处理，注册定时器，每天00：00：00 如果curCount的值是0，需要注册一个定时器，到时第二天将它清空
      if (curCount == 0) {
        val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果达到则加入黑名单
      if (curCount >= maxCount){
        // 判断是否发送过黑名单，只发送一次
        if (!isSentBlackList.value()){
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
        }
        return
      }

      // 计数状态加1， 输出数据到主流
      countState.update(curCount + 1)
      collector.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }
}

// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// 自定义窗口处理函数
class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}