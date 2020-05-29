package flink_perf

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector

object cogroupFunctions {
  def cgfFullOuter[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                        idFromX:X=>String, idFromY:Y=>String,
                        tsFromX:X=>Long, tsFromY:Y=>Long) = new CoGroupFunction[X, Y, (Option[X], Option[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(Option[X], Option[Y])]): Unit = {
      val mp = versionDeduplicator.dedupeFullOuterSeq(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY, xs, ys)
      for (v <- mp) {
        if(v._1.isEmpty) {
          for(y <- v._2) {
            out.collect(None,Some(y))
          }
        } else {
          for(x <- v._1) {
            if(v._2.isEmpty)
              out.collect(Some(x),None)
            else {
              for(y <- v._2) {
                out.collect(Some(x),Some(y))
              }
            }
          }
        }
      }
    }
  }
  // TODO: rename to better reflect that this doesn't allow multiplicity
  // on the left hand side
  def cgfLeftOuter[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                        idFromX:X=>String, idFromY:Y=>String,
                        tsFromX:X=>Long, tsFromY:Y=>Long) = new CoGroupFunction[X, Y, (X, Option[Y])]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(X, Option[Y])]): Unit = {
      val mp = versionDeduplicator.dedupeLeftOuterSeq(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY, xs, ys)
      for(v <- mp) {
        if (v._2.isEmpty)
          out.collect((v._1, None))
        else {
          for (y <- v._2)
            out.collect((v._1, Some(y)))
        }
      }
    }
  }
  // TODO: rename to better reflect that this doesn't allow multiplicity
  // on the left hand side
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
  def cgfInner[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                        idFromX:X=>String, idFromY:Y=>String,
                        tsFromX:X=>Long, tsFromY:Y=>Long) = new CoGroupFunction[X, Y, (X, Y)]() {
    override def coGroup(xs: java.lang.Iterable[X], ys: java.lang.Iterable[Y], out: Collector[(X, Y)]): Unit = {
      val mp = versionDeduplicator.dedupeFullOuterSeq(keyFromX, keyFromY, idFromX, idFromY, tsFromX, tsFromY, xs, ys)
      for(v <- mp) {
        if (!v._1.isEmpty && !v._2.isEmpty) {
          for (x <- v._1)
            for (y <- v._2)
              out.collect((x, y))
        }
      }
    }
  }
}
