package com.cloudera.flink.upgrade

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.cloudera.flink.upgrade.SavepointForRestore.env
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 * 功能描述: 演示开启Checkpoint之后,Flink默认会在停止作业时候删除 Checkpoint 文件，某些情况我们期望保留 Checkpoint 文件，用于恢复作业。
 * 操作步骤:
 *        1. 直接运行程序，作业停止之后，Checkpoint文件删除。
 *        2. 创建目录 tmp/chkdir， 开启停止作业保留Checkpoint状态之后，作业停止之后，Checkpoint文件仍然保留。
 */
object KeepCheckpointForRestore extends App {

  val log = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  // 设置重启策略
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)))

  // 打开Checkpoint
  env.enableCheckpointing(20)

  val stateBackend: StateBackend = new FsStateBackend("file:///E:/CDHProjectDemo/flink-demo/tmp/chkdir", false)
  env.setStateBackend(stateBackend)

  // 停止作业保留CP文件
  env.getCheckpointConfig.enableExternalizedCheckpoints(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


  val source = env.addSource(new SourceFunction[(String, Int, Long)]() {
    override def run(ctx: SourceFunction.SourceContext[(String, Int, Long)]): Unit = {
      var index = 1
      while (true) {
        ctx.collect("key", index, System.currentTimeMillis())
        index = index + 1
        Thread.sleep(100)
      }
    }

    override def cancel(): Unit = {
    }
  })

  source.map(new MapFunction[(String, Int, Long), (String, Int, String)] {

    override def map(event: (String, Int, Long)): (String, Int, String) = {
      if (event._2 % 10 == 0) {
        val msg = s"Bad data [${event._2}]..."
        log.error(msg)
        // 抛出异常，作业根据 配置 的重启策略进行恢复，无重启策略作业直接退出。
        throw new RuntimeException(msg)
      }
      (event._1, event._2, new Timestamp(System.currentTimeMillis).toString)
    }
  }).keyBy(0)
    .sum(1)
    .print()

  env.execute("CheckpointForFailover Job")
}
