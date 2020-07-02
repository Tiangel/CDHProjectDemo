package restart

import java.sql.Timestamp

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 * 功能描述: 演示没有设置重启策略时候，Flink作业异常后的行为 - 直接退出。
 * 操作步骤:
 *        1. 直接运行程序，当作业打印出99之后，作业退出。
 *        2. 增加env.setRestartStrategy(RestartStrategies.noRestart());观察行为和默认一样。
 */
object NoRestartJob extends App {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  env.setRestartStrategy(RestartStrategies.noRestart)

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

      if (event._2 % 100 == 0) {
        val msg = s"Bad data [${event._2}]..."
        log.error(msg)
        // 抛出异常，作业根据 配置 的重启策略进行恢复，无重启策略作业直接退出。
        throw new RuntimeException(msg)
      }
      (event._1, event._2, new Timestamp(System.currentTimeMillis).toString)
    }
  }).print()

  env.execute("NoRestart Job")

}
