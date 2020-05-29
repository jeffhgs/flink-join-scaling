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
      .apply(cogroupFunctions.cgfFullOuter[X,Y])
    joinxy
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
      .apply(cogroupFunctions.cgfLeftOuter[X,Y])
    joinxy
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
      .apply(cogroupFunctions.cgfLeftOuterSeq[X,Y](keyFromX, keyFromY,
        idFromX, idFromY,
        tsFromX, tsFromY))
    joinxy
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
      .apply(cogroupFunctions.cgfFullOuterSeq[X,Y](keyFromX, keyFromY,
        idFromX, idFromY,
        tsFromX, tsFromY))
    joinxy
  }
}
