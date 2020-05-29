package flink_perf

import scala.collection.mutable
import collection.JavaConverters._

object versionDeduplicator {
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
    mp.values.map(v => (v._1, v._2.values))
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
}
