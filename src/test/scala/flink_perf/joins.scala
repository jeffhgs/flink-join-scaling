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
  case class JoinFullOuterByCogroup(tFromX:(A => Long), tFromY:(B=>Long)) {
    import scala.collection.JavaConversions._
    def join(x:DataStream[A], y:DataStream[B])(implicit _tix : TypeInformation[A], _tiy : TypeInformation[B]) : DataStream[(Option[A],Option[B])] = {
      implicit val _tixy = createTypeInformation[(A,B)]
      implicit val _tixy2 = createTypeInformation[(Option[A], Option[B])]
      x.keyBy(_.id).coGroup(y.keyBy(_.ida))
        .where(_.id)
        .equalTo(_.id)
        .window(GlobalWindows.create())
        .trigger(CountTrigger.of(1))
        .apply(cgf1)(_tixy2)
    }
  }
}
