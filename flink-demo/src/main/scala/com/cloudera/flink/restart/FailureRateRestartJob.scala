package restart

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}


/**
 * 功能描述: 演示开启failureRate的重启策略，Flink作业异常后的行为。
 * 操作步骤:
 *        1. 直接运行程序，failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(5, TimeUnit.SECONDS)))
 *           作业5分钟内5次失败，则终止作业，每次启动间隔5秒钟，当在5分钟内出现第5次异常后，终止作业。
 *        2. 修改策略failureRateRestart(5, Time.of(5, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS)))，
 *           也即是 5秒内如果有5次异常，则终止作业。本测试将会不停的重启，因为 5 秒内始终不会达到5次异常。
 */
object FailureRateRestartJob extends App {
  val log = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  env.setRestartStrategy(
    RestartStrategies.failureRateRestart(
      5, Time.of(5, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS)
    )
  )


  val source = env.addSource(new SourceFunction[(String, Int, Long)]() {
    override def run(ctx: SourceFunction.SourceContext[(String, Int, Long)]): Unit = {
      var index = 1
      while (true) {
        index = index + 1
        ctx.collect("key", index, System.currentTimeMillis())
        Thread.sleep(200)
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
  }).print()

  env.execute("FixedDelayRestart Job")

}
