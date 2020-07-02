package com.cloudera.flink.restart

import java.sql.Timestamp

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 * 功能描述: 演示开启Checkpoint之后，Flink作业异常后的默认重启作业策略（固定频率）。
 * 操作步骤:
 *        1. 直接运行程序，env.enableCheckpointing(2000)之后，作业是fixed-delay重启策略。
 */
object EnableCheckpointRestartJob extends App {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  env.enableCheckpointing(2000)

  val source: DataStream[(String, Int, Long)] = env.addSource(new SourceFunction[(String, Int, Long)]() {
    override def run(ctx: SourceFunction.SourceContext[(String, Int, Long)]): Unit = {
      var index: Int = 1
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
        val msg: String = s"Bad data [${event._2}]..."
        log.error(msg)
        // 抛出异常，作业根据 配置 的重启策略进行恢复，无重启策略作业直接退出。
        throw new RuntimeException(msg)
      }
      (event._1, event._2, new Timestamp(System.currentTimeMillis).toString)
    }
  }).print()

  env.execute("FixedDelayRestart Job")
}
