package com.lei.mall;


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-04-29 10:06
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class MallCalculate {
    public static void main(String[] args){
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);
//
//
//        Properties consumerProps = ParameterUtil.getFromResourceFile("kafka.properties");
//        DataStream<String> sourceStream = env
//                .addSource(new FlinkKafkaConsumer<>(
//                        "",                        // topic
//                        new SimpleStringSchema(),                    // deserializer
//                        consumerProps                                // consumer properties
//                ))
//                .setParallelism(1)
//                .name("source_kafka_") //.name("source_kafka_" + ORDER_EXT_TOPIC_NAME)
//                .uid("source_kafka_"); //.uid("source_kafka_" + ORDER_EXT_TOPIC_NAME);
//
//        DataStream<SubOrderDetail> orderStream = sourceStream
//                .map(message -> JSON.parseObject(message, SubOrderDetail.class))
//                .name("map_sub_order_detail").uid("map_sub_order_detail");
//
//        WindowedStream<SubOrderDetail, Tuple, TimeWindow> siteDayWindowStream = orderStream
//                .keyBy("siteId")
//                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));
//
//
//
//        DataStream<OrderAccumulator> siteAggStream = siteDayWindowStream
//                .aggregate(new OrderAndGmvAggregateFunc())
//                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

    }

}


