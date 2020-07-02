package com.cloudera.flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object MinusStateFunction extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  //创建元素数据集
  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val inputStream: DataStream[(String, Long)] = env.socketTextStream(hostname, port)
    .map(x => {
      val arr: Array[String] = x.split(" ")
      (arr(0),arr(1).toLong)
    })

  inputStream.keyBy(_._1).flatMap(
    //定义和创建RichFlatMapFunction，第一个参数为输入数据类型，第二参数为输出数据类型
    new RichFlatMapFunction[(String, Long), (String, Long, Long)] {

      val ttlConfig: StateTtlConfig = StateTtlConfig
        // 指定 TTL 时长为 1s
        .newBuilder(Time.seconds(5))
        // StateTtlConfig.UpdateType.OnCreateAndWrite -仅在创建和写入时更新 TTL
        // StateTtlConfig.UpdateType.OnReadAndWrite -所有读写操作都更新 TTL
        // 指定 TTL 刷新时只对创建和写入操作有效。
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        // StateTtlConfig.StateVisibility.NeverReturnExpired -状态数据过期就不返回(默认)
        // StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp -状态数据过期但没有被清理，仍然返回
        // 设置状态可见性为永远不返回过期数据
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build()

      private var leastValueState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        //创建ValueStateDescriptor，定义状态名称为leastValue，并指定数据类型
        val leastValueStateDescriptor: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long]("leastValue", classOf[Long])

        leastValueStateDescriptor.enableTimeToLive(ttlConfig)

        //通过getRuntimeContext.getState获取State
        leastValueState = getRuntimeContext.getState[Long](leastValueStateDescriptor)
      }

      override def flatMap(value: (String, Long), out: Collector[(String, Long, Long)]): Unit = {
        //通过value方法从leastValueState中获取最小值
        var leastValue: Long = leastValueState.value()
        if(leastValueState.value() == null){
          leastValue = value._2
        }

        //如果当前指标大于最小值，则直接输出数据元素和最小值
        if (value._2 > leastValue) {
          out.collect((value._1, value._2, leastValue))
        } else {
          //如果当前指标小于最小值，则更新状态中的最小值
          leastValueState.update(value._2)
          //将当前数据中的指标作为最小值输出
          out.collect((value._1, value._2, value._2))
        }
      }
    }).print()

  // the printed output will be (1,4) and (1,5)

  env.execute("Example Managed State Job")

}
