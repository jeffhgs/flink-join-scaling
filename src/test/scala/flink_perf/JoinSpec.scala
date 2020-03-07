package flink_perf

import flink_perf.joins.cgfFullOuter
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

  private def sampleBC(ida:Int, cfg:CfgCardinality) : List[(B, Seq[C])] = {
    val gen = new GenJoinInput(1000000000L, 1000000L, 1000000)
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

  def ktFromOAOB(ab:((Option[A], Option[B]))) = {
    ab match {
      case (Some(a),Some(b)) => (a.id.toString,a.ts + b.ts)
      case (Some(a),None) => (a.id.toString,a.ts)
      case (None,Some(b)) => (b.ida.toString,b.ts)
      case _ => throw new RuntimeException("Internal error")
    }
  }

  def ktFromAOB(ab:(A, Option[B])) = {
    ab match {
      case (a,Some(b)) => (a.id.toString,a.ts + b.ts)
      case (a,None) => (a.id.toString,a.ts)
      case _ => throw new RuntimeException("Internal error")
    }
  }

  registerTest("AB join input gets generated")(new FlinkTestEnv {
    val cfg = CfgCardinality(true)
    val abs = sampleAB(cfg)
    assert(abs.length == numSamples)
    for(z <- abs) {
      z match {
        case (Some(x),Some(y)) =>
          assert(x.id == y.ida)
        case _ =>
      }
    }
  })

  registerTest("AB outer join output is expected")(new FlinkTestEnv {
    val cfg = CfgCardinality(true)
    val abs = sampleAB(cfg)
    val a : List[A] = abs.flatMap(_._1)
    val b : List[B] = abs.flatMap(_._2)
    val dsa = env.fromCollection(a).assignTimestampsAndWatermarks(new ATimestampAsssigner(Time.milliseconds(dtMax)))
    val dsb = env.fromCollection(b).assignTimestampsAndWatermarks(new BTimestampAsssigner(Time.milliseconds(dtMax)))
    val joinab = joins.JoinFullOuter[A,B](dsa, dsb,
      a => a.id.toString, b => b.ida.toString,
      a => a.id.toString, b => b.id.toString,
      a => a.ts, b => b.ts
    )

    val sink = new TestSink1[(Option[A], Option[B])]().withSource(joinab)
    env.execute()
    val actual = new OmnicientDeduplicator[(Option[A], Option[B])](sink.asSeq(), ktFromOAOB).get()
//    GenJoinInput.print(xy, "E")
//    GenJoinInput.print(actual, "A")
    assert(sink.asSeq().length >= numSamples)
    assert(actual.length == numSamples)
  })

  registerTest("AB outer join monitoring")(new FlinkTestEnv {
    val cfg = CfgCardinality(true)
    val abs = sampleAB(cfg)
    val a : List[A] = abs.flatMap(_._1)
    val b : List[B] = abs.flatMap(_._2)
    val dsa = env.fromCollection(a).assignTimestampsAndWatermarks(new ATimestampAsssigner(Time.milliseconds(dtMax)))
    val dsb = env.fromCollection(b).assignTimestampsAndWatermarks(new BTimestampAsssigner(Time.milliseconds(dtMax)))
    val dsa2 = dsa.transform("dsa",StreamMonitor[A](1000L))
    val dsb2 = dsb.transform("dsb",StreamMonitor[B](1000L))
    val joinab = joins.JoinFullOuter[A,B](dsa, dsb,
      a => a.id.toString, b => b.ida.toString,
      a => a.id.toString, b => b.id.toString,
      a => a.ts, b => b.ts
    )
    val joinab2 = joinab.transform("joinab", StreamMonitor(1000L))
    val sink = new TestSink1[(Option[A], Option[B])]().withSource(joinab2)
    env.execute()
    val actual = new OmnicientDeduplicator[(Option[A], Option[B])](sink.asSeq(), ktFromOAOB).get()
    assert(sink.asSeq().length >= numSamples)
    assert(actual.length == numSamples)
  })

  registerTest("AB left outer join output is expected")(new FlinkTestEnv {
    val cfg = CfgCardinality(true)
    val ab = sampleAB(cfg)
    val a : List[A] = ab.flatMap(_._1)
    val b : List[B] = ab.flatMap(_._2)
    val dsa = env.fromCollection(a).assignTimestampsAndWatermarks(new ATimestampAsssigner(Time.milliseconds(dtMax)))
    val dsb = env.fromCollection(b).assignTimestampsAndWatermarks(new BTimestampAsssigner(Time.milliseconds(dtMax)))
    val joinab = joins.JoinLeftOuter[A,B](dsa, dsb,
      a => a.id.toString, b => b.ida.toString,
      a => a.id.toString, b => b.id.toString,
      a => a.ts, b => b.ts
    )
    val sink = new TestSink1[(A, Option[B])]().withSource(joinab)
    env.execute()
    val actual = new OmnicientDeduplicator[(A, Option[B])](sink.asSeq(), ktFromAOB).get()
    val numExpected = ab.count(xy => xy._1.isDefined)
    assert(sink.asSeq().length >= numExpected)
    assert(actual.length == numExpected)
  })

  registerTest("BC join input gets generated")(new FlinkTestEnv {
    val cfg = CfgCardinality(false)
    val ida = 0
    val bcs = sampleBC(ida,cfg)
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
//      println(s"about to test ${(a,bcs)}")
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
    val actual = new OmnicientDeduplicator[(Option[A], Option[B])](xy, ktFromOAOB).get()
    assert(actual.length == 1)
  }
}
