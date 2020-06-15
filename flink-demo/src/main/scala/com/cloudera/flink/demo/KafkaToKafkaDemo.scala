package com.cloudera.flink.demo

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.extensions._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala._

object KafkaToKafkaDemo {

  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val GROUP_ID = "group.id"
  val RETRIES = "retries"
  val TOPICS = "topics"

  def main(args: Array[String]): Unit = {
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(parameterTool)
    // 设置时间模式为时间时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // checkpoint 设置
    env.enableCheckpointing(60000)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    // 高级设置（这些配置也建议写成配置文件中去读取，优先环境变量）
    // 设置 exactly-once 模式, 精确一次
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置 checkpoint 最小间隔 30 s
    checkpointConfig.setMinPauseBetweenCheckpoints(30000)
    // 设置 checkpoint 必须在1分钟内完成，否则会被丢弃
    checkpointConfig.setCheckpointTimeout(60000)

    // cancel 时保留 checkpoint
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置 statebackend 为 rockdb
    val stateBackend: StateBackend = new RocksDBStateBackend("")
    env.setStateBackend(stateBackend)

    // 设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))
    import scala.collection.JavaConverters._




    //    //设置 checkpoint 周期时间
//    env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000))
//
//    //高级设置（这些配置也建议写成配置文件中去读取，优先环境变量）
//    // 设置 exactly-once 模式
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    // 设置 checkpoint 最小间隔 500 ms
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
//    // 设置 checkpoint 必须在1分钟内完成，否则会被丢弃
//    env.getCheckpointConfig.setCheckpointTimeout(60000)
//    // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
//    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//    // 设置 checkpoint 的并发度为 1
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)







  }
}
