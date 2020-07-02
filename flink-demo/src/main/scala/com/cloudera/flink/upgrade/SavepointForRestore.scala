package com.cloudera.flink.upgrade

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory


/**
 * 功能描述: 演示开启Checkpoint之后,failover之后可以从失败之前的状态进行续跑。
 * 操作步骤:
 *        1. mvn 打包
 *        2.下载flink发布包https://www.apache.org/dyn/closer.lua/flink/flink-1.10.1/flink-1.10.1-bin-scala_2.11.tgz
 *        3. 配置 flink-cong.yaml
 *           配置statebackend
 *                - state.backend: filesystem
 *           配置checkpoint&savepoint
 *                - state.checkpoints.dir: file:///tmp/chkdir
 *                - state.savepoints.dir: file:///tmp/chkdir
 *           配置失败重启策略
 *                - restart-strategy: fixed-delay
 *                - restart-strategy.fixed-delay.attempts: 3
 *                - restart-strategy.fixed-delay.delay: 2 s
 *           配置checkpoint保存个数
 *                - state.checkpoints.num-retained: 2
 *           配置local recovery for this state backend
 *                - state.backend.local-recovery: true
 *
 *        4. bin/start-cluster.sh local
 *        5. bin/flink run -m localhost:4000 -c upgrade.SavepointForRestore E:/CDHProjectDemo/flink-demo/target/flink-demo-0.0.1-SNAPSHOT.jar
 *        6. bin/flink run -m localhost:4000 -s file:///E:/CDHProjectDemo/flink-demo/tmp/chkdir/caab8d0a04aa0ce718da5333cad10607/chk-364
 *           -c upgrade.SavepointForRestore E:/CDHProjectDemo/flink-demo/target/flink-demo-0.0.1-SNAPSHOT.jar
 *        7. 将程序去除异常，运行之后，触发 savepoint
 */
object SavepointForRestore extends App {

  val log = LoggerFactory.getLogger(this.getClass)

  //  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 本地运行 UI
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

  env.setParallelism(1)

  // 设置重启策略
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)))

  // 打开Checkpoint, 我们也可以用 -D <property=value> CLI设置
  env.enableCheckpointing(20)
  // 作业停止后保留CP文件
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
