package com.cloudera.flink.upgrade

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.cloudera.flink.restart.NoRestartJob.{env, log}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 * 功能描述: 演示开启Checkpoint之后,failover之后可以从失败之前的状态进行续跑。
 * 操作步骤:
 *        1. 直接运行程序，failover之后，聚合值可以从新计算。
 *        2. 开启Checkpoint之后，failover之后 聚合值基于原有状态进行续跑。
 *        3. 改变日志级别，我们看一下Checkpoint数据跑哪里去了(No state backend | failover regions.)
 *        4. 方便查看，修改日志WARN，重启作业，计算是重新开始的，说明没有基于Checkpoint的state文件恢复，
 * 默认Checkpoint的文件昨天停止后会删除的。
 */
object EnableCheckpointForFailover extends App {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)))

  //        env.enableCheckpointing(20)

  val source: DataStream[(String, Int, Long)] = env.addSource(new SourceFunction[(String, Int, Long)]() {
    override def run(ctx: SourceFunction.SourceContext[(String, Int, Long)]): Unit = {
      var index = 1
      while (true) {
        index = index + 1
        ctx.collect("key", index, System.currentTimeMillis())
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
