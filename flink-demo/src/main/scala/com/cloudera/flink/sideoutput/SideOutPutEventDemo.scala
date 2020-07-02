package com.cloudera.flink.sideoutput


import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * 侧输出流
 */
object SideOutPutEventDemo extends App {


  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777
  val TAG_1: String = "tag-1"
  val TAG_2: String = "tag-2"

  val OUTPUT_TAG_1 = OutputTag[(String, String)]("side-output-tag-1")
  val OUTPUT_TAG_2 = OutputTag[(String, String)]("side-output-tag-2")

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


  val socketDs: DataStream[String] = streamEnv.socketTextStream(hostname, port)

  val tagStream: DataStream[(String, String)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map(record => {
      record.length % 5 match {
        case 0 => (TAG_1, record)
        case 1 => (TAG_2, record)
        case _ => ("default", record)
      }
    })

  val sideOutputData: DataStream[(String, String)] = tagStream.process(new ProcessFunction[(String, String), (String, String)]() {
    override def processElement(value: (String, String),
                                ctx: ProcessFunction[(String, String), (String, String)]#Context,
                                out: Collector[(String, String)]): Unit = {
      value._1 match {
        case TAG_1 => {
          ctx.output(OUTPUT_TAG_1, value)
        }
        case TAG_2 => {
          ctx.output(OUTPUT_TAG_2, value)
        }
        case _ => {
          out.collect(value)
        }
      }
    }
  })

  sideOutputData.getSideOutput(OUTPUT_TAG_1).print(TAG_1)
  sideOutputData.getSideOutput(OUTPUT_TAG_2).print(TAG_2)
  sideOutputData.print("default")

  /*
  输入数据: nc -lk 7777
      flink spark kafka hive hadoop impala druid zookeeper drois hue sentry
  运行结果:
      tag-1:4> (tag-1,flink)
      tag-1:4> (tag-1,spark)
      tag-1:4> (tag-1,kafka)
      default:4> (default,hive)
      tag-2:4> (tag-2,hadoop)
      tag-2:4> (tag-2,impala)
      tag-1:4> (tag-1,druid)
      default:4> (default,zookeeper)
      tag-1:4> (tag-1,drois)
      default:4> (default,hue)
      tag-2:4> (tag-2,sentry)
   */

  streamEnv.execute("Side OutPut Demo Execute")
}
