package flink_perf

import collection.JavaConversions._
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.util.Collector

object joins {
//  val foo = new java.util.ArrayList().asScala
  def cgf1 = new CoGroupFunction[A, B, (Option[A], Option[B])]() {
    override def coGroup(xs: java.lang.Iterable[A], ys: java.lang.Iterable[B], out: Collector[(Option[A], Option[B])]): Unit = {
      (xs.iterator().hasNext, ys.iterator().hasNext) match {
        case (false, true) =>
          for(y <- ys.iterator().toIterator)
            out.collect((None,Some(y)))
        case (true, false) =>
          for(x <- xs.iterator())
            out.collect((Some(x),None))
        case (true, true) =>
          // TODO: check for degenerate join?
          for(x <- xs.iterator()) {
            for(y <- ys.iterator()) {
              out.collect((Some(x),Some(y)))
            }
          }
        case (false, false) =>
        // do nothing
      }
    }
  }
}
