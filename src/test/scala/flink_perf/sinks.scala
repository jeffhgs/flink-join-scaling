package flink_perf

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable.ArrayBuffer

object sinks {
  case class TestSink1[T <: AnyRef]() extends SinkFunction[T] {
    TestSink1.values.clear()
    def withSource(ds:DataStream[T]) = {
      ds.addSink(this)
      this
    }
    override def invoke(value: T): Unit = {
      synchronized {
        TestSink1.values.append(value.asInstanceOf[AnyRef])
      }
    }
    def asSeq() : Seq[T]= {
      TestSink1.values.map(_.asInstanceOf[T])
    }
  }
  object TestSink1 {
    // must be static
    private[sinks] val values = ArrayBuffer[AnyRef]()
  }
}
