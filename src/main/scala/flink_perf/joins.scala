package flink_perf

import collection.JavaConverters._
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.util.Collector

import scala.collection.mutable

object joins {
  def cgfFullOuter[X,Y] = new CoGroupFunction[X, Y, (Option[X], Option[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(Option[X], Option[Y])]): Unit = {
      (xs.iterator().hasNext, ys.iterator().hasNext) match {
        case (false, true) =>
          for(y <- ys.asScala.iterator)
            out.collect((None,Some(y)))
        case (true, false) =>
          for(x <- xs.asScala.iterator)
            out.collect((Some(x),None))
        case (true, true) =>
          // TODO: check for degenerate join?
          for(x <- xs.asScala.iterator) {
            for(y <- ys.asScala.iterator) {
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
      .evictor(new VersioningEvictor(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY))
      .apply(cgfFullOuter[X,Y])
    joinxy
  }
  def cgfLeftOuter[X,Y] = new CoGroupFunction[X, Y, (X, Option[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(X, Option[Y])]): Unit = {
      (xs.asScala.iterator.hasNext, ys.asScala.iterator.hasNext) match {
        case (false, true) =>
        case (true, false) =>
          for(x <- xs.asScala.iterator)
            out.collect((x,None))
        case (true, true) =>
          // TODO: check for degenerate join?
          for(x <- xs.asScala.iterator) {
            for(y <- ys.asScala.iterator) {
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
      .evictor(new VersioningEvictor(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY))
      .apply(cgfLeftOuter[X,Y])
    joinxy
  }

  def dedupeLeftOuterSeq[X,Y](keyFromX: X => String, keyFromY: Y => String, idFromX: X => String, idFromY: Y => String, tsFromX: X => Long, tsFromY: Y => Long, xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y]) = {
    val mp = new mutable.HashMap[String, (Option[X], mutable.HashMap[String, Y])]()
    for (x <- xs.asScala.iterator) {
      val key = keyFromX(x)
      val vPrev: (Option[X], mutable.HashMap[String, Y]) = mp.getOrElseUpdate(key, (Some(x), new mutable.HashMap[String, Y]()))
      if (vPrev._1.isDefined && (tsFromX(x) > tsFromX(vPrev._1.get)))
        mp.update(key, (Some(x), vPrev._2))
    }
    for (y <- ys.asScala.iterator) {
      val key = keyFromY(y)
      val idy = idFromY(y)
      val v: (Option[X], mutable.HashMap[String, Y]) = mp.getOrElseUpdate(key, (None, new mutable.HashMap[String, Y]()))
      val yPrev: Y = v._2.getOrElseUpdate(idy, y)
      if (tsFromY(y) > tsFromY(yPrev))
        v._2.update(idy, y)
    }
//    val numX = mp.size
//    val numY = mp.values.map(ab => ab._2.size).sum
//    val vs = mp.values.map(v => (v._1, v._2.values.toSeq))
    mp.values.map(v => (v._1, v._2.values))
  }

  def cgfLeftOuterSeq[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                           idFromX:X=>String, idFromY:Y=>String,
                           tsFromX:X=>Long, tsFromY:Y=>Long) = new CoGroupFunction[X, Y, (X, Seq[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(X, Seq[Y])]): Unit = {
      val mp = dedupeLeftOuterSeq(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY, xs, ys)
      for(v <- mp) {
        if (v._1.isDefined)
          out.collect((v._1.get, v._2.toSeq))
      }
    }
  }


  def JoinLeftOuterSeq[X,Y](dsx2: DataStream[X], dsy2: DataStream[Y],
                            keyFromX:X=>String, keyFromY:Y=>String,
                            idFromX:X=>String, idFromY:Y=>String,
                            tsFromX:X=>Long, tsFromY:Y=>Long)(implicit _tiX:TypeInformation[X], _tiY:TypeInformation[Y]) = {
    val _tiXY = createTypeInformation[(X, Option[Y])]
    val joinxy = dsx2.keyBy(keyFromX).coGroup(dsy2.keyBy(keyFromY))
      .where(keyFromX)
      .equalTo(keyFromY)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .evictor(new VersioningEvictor(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY))
      .apply(cgfLeftOuterSeq[X,Y](keyFromX, keyFromY,
        idFromX, idFromY,
        tsFromX, tsFromY))
    joinxy
  }

  def dedupeFullOuterSeq[X,Y](keyFromX: X => String, keyFromY: Y => String, idFromX: X => String, idFromY: Y => String, tsFromX: X => Long, tsFromY: Y => Long, xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y]) = {
    val mp = new mutable.HashMap[String, (mutable.HashMap[String, X], mutable.HashMap[String, Y])]()
    for (x <- xs.asScala.iterator) {
      val key = keyFromX(x)
      val idx = idFromX(x)
      val v: (mutable.HashMap[String, X], mutable.HashMap[String, Y]) =
        mp.getOrElseUpdate(key, (new mutable.HashMap[String, X](), new mutable.HashMap[String, Y]()))
      val xPrev: X = v._1.getOrElseUpdate(idx, x)
      if (tsFromX(x) > tsFromX(xPrev))
        v._1.update(idx, x)
    }
    for (y <- ys.asScala.iterator) {
      val key = keyFromY(y)
      val idy = idFromY(y)
      val v: (mutable.HashMap[String, X], mutable.HashMap[String, Y]) =
        mp.getOrElseUpdate(key, (new mutable.HashMap[String, X](), new mutable.HashMap[String, Y]()))
      val yPrev: Y = v._2.getOrElseUpdate(idy, y)
      if (tsFromY(y) > tsFromY(yPrev))
        v._2.update(idy, y)
    }
    mp.values.map(v => (v._1.values, v._2.values))
  }

  def cgfFullOuterSeq[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                           idFromX:X=>String, idFromY:Y=>String,
                           tsFromX:X=>Long, tsFromY:Y=>Long) = new CoGroupFunction[X, Y, (Seq[X], Seq[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(Seq[X], Seq[Y])]): Unit = {
      val mp = dedupeFullOuterSeq(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY, xs, ys)
      for (v <- mp) {
        out.collect((v._1.toSeq, v._2.toSeq))
      }
    }
  }

  def JoinFullOuterSeq[X,Y](dsx2: DataStream[X], dsy2: DataStream[Y],
                            keyFromX:X=>String, keyFromY:Y=>String,
                            idFromX:X=>String, idFromY:Y=>String,
                            tsFromX:X=>Long, tsFromY:Y=>Long)(implicit _tiX:TypeInformation[X], _tiY:TypeInformation[Y]) = {
    val joinxy = dsx2.keyBy(keyFromX).coGroup(dsy2.keyBy(keyFromY))
      .where(keyFromX)
      .equalTo(keyFromY)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .evictor(new VersioningEvictor(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY))
      .apply(cgfFullOuterSeq[X,Y](keyFromX, keyFromY,
        idFromX, idFromY,
        tsFromX, tsFromY))
    joinxy
  }
}