package com.cloudera.flink.upgrade

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory


/**
 * 功能描述: 演示开启Checkpoint之后,failover之后可以从失败之前的状态进行续跑。
 * 操作步骤:
 *        0. 进行SavepointForRestore的测试之后，进行这个测试
 *        1. 打包： mvn clean package
 *        2. 其中作业：bin/flink run -d -m localhost:4000 -c upgrade.SavepointForRestoreWithoutException /Users/jincheng.sunjc/work/know_how_know_why/khkw/No25-upgrade/target/No25-upgrade-0.1.jar
 *        3.创建savepoint： bin/flink savepoint 1fb52d2d72906045dbba2ce4199f245b
 *        4. 停止以前的作业，然后从savepoint启动
 *        6. bin/flink run -m localhost:4000 -s file:///tmp/chkdir/savepoint-1fb52d-126a8a0e36c3
 *          -c upgrade.SavepointForRestoreWithoutException /Users/jincheng.sunjc/work/know_how_know_why/khkw/No25-upgrade/target/No25-upgrade-0.1.jar \
 */
object SavepointForRestoreWithoutException extends App {

  val log = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  // 设置重启策略
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)))

  // 打开Checkpoint
  env.enableCheckpointing(20)

  env.setStateBackend(new Nothing("file:///tmp/chkdir", false))

  // 停止作业保留CP文件
  //        env.getCheckpointConfig().enableExternalizedCheckpoints(
  //                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


  val source = env.addSource(new SourceFunction[(String, Int, Long)]() {
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
