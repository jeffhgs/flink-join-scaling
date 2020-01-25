package flink_perf

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


trait FlinkTestEnv {
  val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
  //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  // alternatively:
  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
  // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  lazy val out = new TestCollector[String]
}
