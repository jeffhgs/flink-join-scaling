package flink_perf

import java.lang

import org.apache.flink.streaming.api.datastream.CoGroupedStreams
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

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
    mp.values.flatMap(v =>
      v._1 match {
        case None => Seq()
        case Some(x) => Seq((x,v._2.values))
      }
    )
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

  def deduplicateMutable[X,Y](
                                  idFromX: X => String, idFromY: Y => String, tsFromX: X => Long, tsFromY: Y => Long,
                                  elements: lang.Iterable[TimestampedValue[CoGroupedStreams.TaggedUnion[X, Y]]]): Unit
  = {
    val newestX = mutable.Map[String, Long]()
    val newestY = mutable.Map[String, Long]()
    val iter1 = elements.iterator()
    while (iter1.hasNext()) {
      val e = iter1.next().getValue
      if (e.isOne()) {
        val x = e.getOne()
        val id = idFromX(x)
        val tsOld = newestX.getOrElseUpdate(id, tsFromX(x))
        val ts = tsFromX(x)
        if (ts > tsOld) {
          newestX.update(id, ts)
        }
      } else {
        val y = e.getTwo()
        val id = idFromY(y)
        val tsOld = newestY.getOrElseUpdate(id, tsFromY(y))
        val ts = tsFromY(y)
        if (ts > tsOld) {
          newestY.update(id, ts)
        }
      }
    }
    val iter2 = elements.iterator()
    while (iter2.hasNext()) {
      val e = iter2.next().getValue
      if (e.isOne()) {
        val x = e.getOne()
        val id = idFromX(x)
        val tsMax = newestX.getOrElse(id, Long.MinValue)
        val ts = tsFromX(x)
        if (ts < tsMax) {
          iter2.remove()
        }
      } else {
        val y = e.getTwo()
        val id = idFromY(y)
        val tsMax = newestX.getOrElse(id, Long.MinValue)
        val ts = tsFromY(y)
        if (ts < tsMax) {
          iter2.remove()
        }
      }
    }
    false
  }
}
