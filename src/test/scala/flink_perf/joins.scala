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
  def cgfFullOuter[X,Y] = new CoGroupFunction[X, Y, (Option[X], Option[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(Option[X], Option[Y])]): Unit = {
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
  def JoinFullOuter[X,Y](dsx2: DataStream[X], dsy2: DataStream[Y],
                         keyFromX:X=>String, keyFromY:Y=>String,
                         idFromX:X=>String, idFromY:Y=>String,
                         tsFromX:X=>Long, tsFromY:Y=>Long)(implicit _tiX:TypeInformation[X], _tiY:TypeInformation[Y]) = {
    val _tiXY = createTypeInformation[(Option[X], Option[Y])]
    val joinxy = dsx2.keyBy(keyFromX).coGroup(dsy2.keyBy(keyFromY))
      .where(keyFromX)
      .equalTo(keyFromY)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .apply(cgfFullOuter[X,Y])
    joinxy
  }
  def cgfLeftOuter[X,Y] = new CoGroupFunction[X, Y, (X, Option[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(X, Option[Y])]): Unit = {
      (xs.iterator().hasNext, ys.iterator().hasNext) match {
        case (false, true) =>
        case (true, false) =>
          for(x <- xs.iterator())
            out.collect((x,None))
        case (true, true) =>
          // TODO: check for degenerate join?
          for(x <- xs.iterator()) {
            for(y <- ys.iterator()) {
              out.collect((x,Some(y)))
            }
          }
        case (false, false) =>
        // do nothing
      }
    }
  }
  def JoinLeftOuter[X,Y](dsx2: DataStream[X], dsy2: DataStream[Y],
                         keyFromX:X=>String, keyFromY:Y=>String,
                         idFromX:X=>String, idFromY:Y=>String,
                         tsFromX:X=>Long, tsFromY:Y=>Long)(implicit _tiX:TypeInformation[X], _tiY:TypeInformation[Y]) = {
    val _tiXY = createTypeInformation[(X, Option[Y])]
    val joinxy = dsx2.keyBy(keyFromX).coGroup(dsy2.keyBy(keyFromY))
      .where(keyFromX)
      .equalTo(keyFromY)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .apply(cgfLeftOuter[X,Y])
    joinxy
  }
}
