package com.lei.networkflow_analysis


import java.text.SimpleDateFormat
import java.util

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:10 下午 2020/4/23
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 输入数据样例类
case class ApacheLogEvent(ip:String, userId:String, eventTime:Long, method:String, url:String)

// 窗口聚合结果样例类
case class UrlViewCount(url:String, windowEnd:Long, count:Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    // 1、创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置处理时间语义为eventTime，即事件生成时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2、读取数据
    val dataStream: DataStream[ApacheLogEvent] = env.readTextFile("src/main/resources/apache.log")
      .map(data => {
        val dataArray: Array[String] = data.split(" ")
      // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim,  timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      // 如果数据是乱序的，使用 assignTimestampsAndWatermarks
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))// 窗口滑动
      .allowedLateness(Time.seconds(60)) // 允许60秒数据延迟
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrl(5))


    dataStream.print()

    env.execute("network flow job")
  }
}


// 自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


// 自定义排序输出处理函数
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从状态中拿到数据
    val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]
    val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()
    while (iter.hasNext){
      allUrlViews += iter.next()
    }

    urlState.clear()

    val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count)

    // 格式化结果输出
    val result: StringBuffer = new StringBuffer()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for(i <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      result.append("NO").append(i + 1).append(":")
        .append(" URL=").append(currentUrlView.url)
        .append(" 访问量=").append(currentUrlView.count).append("\n")
    }

    result.append("===============================")
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}


