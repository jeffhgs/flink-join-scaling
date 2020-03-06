package flink_perf

import flink_perf.joins.cgf1
import flink_perf.sinks.TestSink1
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class JoinSpec extends AnyFunSuite {
  val seed = Seed(123)
  val numSamples = 100
  val dtMax = 10000L

  private def sampleAB(cfg:CfgCardinality) : List[(Option[A], Option[B])] = {
    val gen = new GenJoinInput(1000000000L, 1000000L, 1000000)
    val cfgSimple = cfg.copy(rightDist=CfgUniform(1))
    val gen2 = gen.genABPairNonemptyNoseq(cfgSimple)
    GenUtil.sampleExactlyN[(Option[A], Option[B])](gen2, seed, numSamples)
  }

  private def sampleBC(cfg:CfgCardinality) : List[(B, Seq[C])] = {
    val gen = new GenJoinInput(1000000000L, 1000000L, 1000000)
    val ida = 0
    val cfgBC = CfgUniform(2)
    val gen2 = gen.genBC(ida, cfg.rightDist)
    GenUtil.sampleExactlyN[(B, Seq[C])](gen2, seed, numSamples)
  }

  private def sampleABC(cfg:CfgCardinality) : List[(A, Seq[(B, Seq[C])])] = {
    val gen = new GenJoinInput(1000000000L, 1000000L, 1000000)
    val cfgSimple = cfg.copy(rightDist=CfgUniform(1))
    val gen2 = gen.genABC(cfgSimple, CfgUniform(2))
    GenUtil.sampleExactlyN[(A, Seq[(B, Seq[C])])](gen2, seed, numSamples)
  }

  def ktFromXY(ab:((Option[A], Option[B]))) = {
    ab match {
      case (Some(a),Some(b)) => (a.id.toString,a.ts + b.ts)
      case (Some(a),None) => (a.id.toString,a.ts)
      case (None,Some(b)) => (b.ida.toString,b.ts)
      case _ => throw new RuntimeException("Internal error")
    }
  }

  registerTest("AB join input gets generated")(new FlinkTestEnv {
    val cfg = CfgCardinality(true)
    val xy = sampleAB(cfg)
    assert(xy.length == numSamples)
    for(z <- xy) {
      z match {
        case (Some(x),Some(y)) =>
          assert(x.id == y.ida)
        case _ =>
      }
    }
  })

  registerTest("AB join output is expected")(new FlinkTestEnv {
    val cfg = CfgCardinality(true)
    val xy = sampleAB(cfg)
    val x : List[A] = xy.flatMap(_._1)
    val y : List[B] = xy.flatMap(_._2)
    val dsx = env.fromCollection(x).assignTimestampsAndWatermarks(new ATimestampAsssigner(Time.milliseconds(dtMax)))
    val dsy = env.fromCollection(y).assignTimestampsAndWatermarks(new BTimestampAsssigner(Time.milliseconds(dtMax)))
    val joinxy = dsx.keyBy(_.id).coGroup(dsy.keyBy(_.ida))
      .where(_.id)
      .equalTo(_.ida)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .apply(cgf1)

    val sink = new TestSink1[(Option[A], Option[B])]().withSource(joinxy)
    env.execute()
    val actual = new OmnicientDeduplicator[(Option[A], Option[B])](sink.asSeq(), ktFromXY).get()
    GenJoinInput.print(xy, "E")
    GenJoinInput.print(actual, "A")
    assert(sink.asSeq().length >= numSamples)
    assert(actual.length == numSamples)
  })

  registerTest("AB join monitoring")(new FlinkTestEnv {
    val cfg = CfgCardinality(true)
    val xy = sampleAB(cfg)
    val x : List[A] = xy.flatMap(_._1)
    val y : List[B] = xy.flatMap(_._2)
    val dsx = env.fromCollection(x).assignTimestampsAndWatermarks(new ATimestampAsssigner(Time.milliseconds(dtMax)))
    val dsy = env.fromCollection(y).assignTimestampsAndWatermarks(new BTimestampAsssigner(Time.milliseconds(dtMax)))
    val dsx2 = dsx.transform("dsx",StreamMonitor[A](1000L))
    val dsy2 = dsy.transform("dsy",StreamMonitor[B](1000L))
    val joinxy = dsx2.keyBy(_.id).coGroup(dsy2.keyBy(_.ida))
      .where(_.id)
      .equalTo(_.ida)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .apply(cgf1)
    val joinxy2 = joinxy.transform("joinxy", StreamMonitor(1000L))
    val sink = new TestSink1[(Option[A], Option[B])]().withSource(joinxy2)
    env.execute()
    val actual = new OmnicientDeduplicator[(Option[A], Option[B])](sink.asSeq(), ktFromXY).get()
    assert(sink.asSeq().length >= numSamples)
    assert(actual.length == numSamples)
  })

  registerTest("BC join input gets generated")(new FlinkTestEnv {
    val cfg = CfgCardinality(false)
    val bcs = sampleBC(cfg)
    assert(bcs.length == numSamples)
    for ((b, cs) <- bcs){
      for(c <- cs) {
        assert(c.idb == b.id)
      }
    }
  })

  registerTest("ABC join input gets generated")(new FlinkTestEnv {
    val cfg = CfgCardinality(false)
    val abc = sampleABC(cfg)
    assert(abc.length == numSamples)
    for((a,bcs) <- abc) {
      println(s"about to test ${(a,bcs)}")
      for {(b, cs) <- bcs;
           _ = assert(b.ida == a.id)
      }{
        for(c <- cs) {
          assert(c.idb == b.id)
        }
      }
    }
  })

  test("Deduplicator catches duplicate") {
    val xy = List(
      (None, Some(B(914090,999988501, 4059))),
      (Some(A(4059, 999440747)), Some(B(914090,999988501, 4059)))
    )
    val actual = new OmnicientDeduplicator[(Option[A], Option[B])](xy, ktFromXY).get()
    assert(actual.length == 1)
  }
}
