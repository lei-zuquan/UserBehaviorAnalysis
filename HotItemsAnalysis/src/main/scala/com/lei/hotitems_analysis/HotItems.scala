package com.lei.hotitems_analysis

import com.lei.hotitems_analysis.util.MyKafkaUtil
import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:09 下午 2020/4/22
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 实时热门商品统计
 *
 * 模块代码实现
 *    我们将实现一个"实时热门商品"的需求，可以将"实时热门商品" 翻译成程序员更好理解的需求"
 * 每隔5分钟输出最近一小时内点击量多的前N个商品，将这个需求进行分解我们大概要做这么几件事情：
 *        1.抽取业务时间戳，告诉Flink框架基于业务时间做窗口
 *        2.过虑出点击行为数据
 *        3.按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
 *        4.按每个窗口聚合，输出第个窗口中点击量前N名的商品
 *
 * 程序主体：
 *    在 src/main/scala 下创建 HotItems.scala 文件，新建一个单例对象。定义样例类
 *    UserBehavior 和 ItemViewCount，在 main 函数中创建 StreamExecutionEnvironment 并
 *    做配置，然后从 UserBehavior.csv 文件中读取数据，并包装成 UserBehavior 类型。
 */
// 定义输出数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp:Long)
// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 1、创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置处理时间语义为eventTime，即事件生成时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2、读取数据
    val dataStream: DataStream[UserBehavior] = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    //val dataStream: DataStream[UserBehavior] = env.addSource(MyKafkaUtil.getConsumer("hotitems"))
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 数据输入是有序的使用 assignAscendingTimestamps

    // 3、transform 处理数据
    val processStream: DataStream[String] = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult()) // 窗口聚合；统计最近1个小时，热门商品的被点击的次数，并输出到WindowResult(商品id, 窗口end，点击次数)
      .keyBy(_.windowEnd) // 按照窗口分组; 再次按窗口end进行分组，得到的就是同一个window下的不同商品被点击的次数
      .process(new TopNHotItems(3))  // 对同一个window下的不同商品被点击次数进行取TOP

    // 4、sink: 控制台输出
    processStream.print()

    env.execute("hot items job")


  }

}

// CountAgg的输出就是WindowResult的输入
// 自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long =  acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// 自定义窗口函数
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义的处理函数；对同一个窗口下的商品取热门TOP N
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一定定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
  }

  // 定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有state中的数据取出，放到一个list Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()

    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += item
    }

    // 按照count大小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态
    itemState.clear()

    // 将排名结果格式化输出
    val result: StringBuilder = new StringBuilder();
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 输出每一个商品的信息
    for (i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }

    result.append("==================================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}