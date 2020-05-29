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
    if ((size + 1) % numRecordsBetweenCleanup == 0) {
      versionDeduplicator.deduplicateMutable(idFromX, idFromY, tsFromX, tsFromY, elements)
    }
  }

}
