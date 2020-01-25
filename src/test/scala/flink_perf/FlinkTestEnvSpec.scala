package flink_perf

import org.scalatest.{FunSuite}

class FlinkTestEnvSpec extends FunSuite {
  registerTest("FlinkEnv runs")(new FlinkTestEnv {

  })
}
