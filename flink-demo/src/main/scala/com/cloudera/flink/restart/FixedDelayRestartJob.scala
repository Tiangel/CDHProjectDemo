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
 * 功能描述: 演示重启FixedDelay的重启作业，Flink作业异常后的行为以固定的频率重启作业。
 * 操作步骤:
 *        1. 直接运行程序，当作业打印出9之后，抛出异常，重启作业，当第四次打印9之后，作业终止。
 *           观察 重启之后的时间戳，感知 fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)中2秒的含义。
 *
 *        2. 将 Time.of(2, TimeUnit.SECONDS)， 变成 Time.of(3, TimeUnit.SECONDS) 观察作业重启时间变化。
 */
object FixedDelayRestartJob extends App {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

//  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)))

  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(3, TimeUnit.SECONDS)))


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
  }).print()

  env.execute("FixedDelayRestart Job")

}

