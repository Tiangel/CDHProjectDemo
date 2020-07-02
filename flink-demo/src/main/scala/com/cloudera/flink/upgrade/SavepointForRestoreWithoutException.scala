package com.cloudera.flink.upgrade

import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory


/**
 * 功能描述: 演示开启 Checkpoint 之后,failover之后可以从失败之前的状态进行续跑。
 * 操作步骤:
 *        0. 进行 SavepointForRestore 的测试之后，进行这个测试
 *        1. 打包： mvn clean package
 *        2. 其中作业：bin/flink run -d -m localhost:8081 -c upgrade.SavepointForRestoreWithoutException E:/CDHProjectDemo/flink-demo/target/flink-demo-0.0.1-SNAPSHOT.jar
 *        3.创建savepoint： bin/flink savepoint 1fb52d2d72906045dbba2ce4199f245b
 *        4. 停止以前的作业，然后从savepoint启动
 *        6. bin/flink run -m localhost:8081 -s file:///E:/CDHProjectDemo/flink-demo/tmp/chkdir/savepoint-1fb52d-126a8a0e36c3
 *           -c upgrade.SavepointForRestoreWithoutException E:/CDHProjectDemo/flink-demo/target/flink-demo-0.0.1-SNAPSHOT.jar \
 */
object SavepointForRestoreWithoutException extends App {

  val log = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // 本地运行 UI
  //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

  env.setParallelism(1)

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

  source.keyBy(0).sum(1).print()

  env.execute("SavepointForFailoverWithoutException Job")
}
