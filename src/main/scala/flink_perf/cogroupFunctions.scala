package flink_perf

import collection.JavaConverters._
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector

object cogroupFunctions {
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
  def cgfLeftOuterSeq[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                           idFromX:X=>String, idFromY:Y=>String,
                           tsFromX:X=>Long, tsFromY:Y=>Long) = new CoGroupFunction[X, Y, (X, Seq[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(X, Seq[Y])]): Unit = {
      val mp = versionDeduplicator.dedupeLeftOuterSeq(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY, xs, ys)
      for(v <- mp) {
        if (v._2.isEmpty)
          out.collect((v._1, Seq()))
        else
          out.collect((v._1, v._2.toSeq))
      }
    }
  }

  def cgfFullOuterSeq[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                           idFromX:X=>String, idFromY:Y=>String,
                           tsFromX:X=>Long, tsFromY:Y=>Long) = new CoGroupFunction[X, Y, (Seq[X], Seq[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(Seq[X], Seq[Y])]): Unit = {
      val mp = versionDeduplicator.dedupeFullOuterSeq(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY, xs, ys)
      for (v <- mp) {
        out.collect((v._1.toSeq, v._2.toSeq))
      }
    }
  }
}
