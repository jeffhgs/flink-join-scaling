package flink_perf

import java.lang

import org.apache.flink.streaming.api.datastream.CoGroupedStreams
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

import scala.collection.mutable

class VersioningEvictor[X,Y](
                              keyFromX: X => String, keyFromY: Y => String,
                              idFromX: X => String, idFromY: Y => String,
                              tsFromX: X => Long, tsFromY: Y => Long,
                              numRecordsBetweenCleanup : Int = 5
                            ) extends Evictor[CoGroupedStreams.TaggedUnion[X,Y],GlobalWindow]
{
  override def evictBefore(elements: lang.Iterable[TimestampedValue[CoGroupedStreams.TaggedUnion[X, Y]]], size: Int, window: GlobalWindow, evictorContext: Evictor.EvictorContext): Unit = {}

  override def evictAfter(
                           elements: lang.Iterable[TimestampedValue[CoGroupedStreams.TaggedUnion[X, Y]]],
                           size: Int,
                           window: GlobalWindow,
                           evictorContext: Evictor.EvictorContext): Unit =
  {
    if((size+1) % numRecordsBetweenCleanup > 0) {
      return
    }
    val newestX = mutable.Map[String,Long]()
    val newestY = mutable.Map[String,Long]()
    val iter1 = elements.iterator()
    while(iter1.hasNext()) {
      val e = iter1.next().getValue
      if(e.isOne()) {
        val x = e.getOne()
        val id = idFromX(x)
        val tsOld = newestX.getOrElseUpdate(id, tsFromX(x))
        val ts = tsFromX(x)
        if(ts > tsOld) {
          newestX.update(id,ts)
        }
      } else {
        val y = e.getTwo()
        val id = idFromY(y)
        val tsOld = newestY.getOrElseUpdate(id, tsFromY(y))
        val ts = tsFromY(y)
        if(ts > tsOld) {
          newestY.update(id,ts)
        }
      }
    }
    val iter2 = elements.iterator()
    while(iter2.hasNext()) {
      val e = iter2.next().getValue
      if(e.isOne()) {
        val x = e.getOne()
        val id = idFromX(x)
        val tsMax = newestX.getOrElse(id,Long.MinValue)
        val ts = tsFromX(x)
        if(ts < tsMax) {
          iter2.remove()
        }
      } else {
        val y = e.getTwo()
        val id = idFromY(y)
        val tsMax = newestX.getOrElse(id,Long.MinValue)
        val ts = tsFromY(y)
        if(ts < tsMax) {
          iter2.remove()
        }
      }
    }
  }
}
