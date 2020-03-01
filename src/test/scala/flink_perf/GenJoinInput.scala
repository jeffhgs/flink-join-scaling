package flink_perf

import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, TimestampAssigner, TimestampExtractor}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.scalacheck.rng.Seed

sealed abstract class Tree
case class Node(left: Tree, right: Tree, v: Int) extends Tree
case object Leaf extends Tree

object GenTree {
  import org.scalacheck._
  import Gen._
  import Arbitrary.arbitrary

  val genLeaf = const(Leaf)

  val genNode = for {
    v <- arbitrary[Int]
    left <- genTree
    right <- genTree
  } yield Node(left, right, v)

  def genTree: Gen[Tree] = oneOf(genLeaf, lzy(genNode))
}

case class CfgUniform(numMax:Int)
case class CfgCardinality(leftOptional:Boolean, rightDist:CfgUniform=CfgUniform(1))

case class A(id: Int, ts:Long)
case class B(id: Int, ts:Long, ida:Int)

class ATimestampAsssigner(maxDt:Time) extends BoundedOutOfOrdernessTimestampExtractor[A](maxDt) {
  override def extractTimestamp(a: A): Long = {
    a.ts
  }
}

class BTimestampAsssigner(maxDt:Time) extends BoundedOutOfOrdernessTimestampExtractor[B](maxDt) {
  override def extractTimestamp(b: B): Long = {
    b.ts
  }
}


class GenJoinInput(tMax: Long, dtMax: Long, idMax:Int) {
  import org.scalacheck._
  import Gen._
  import Arbitrary.arbitrary

  def genA(ida: Int):Gen[A] = {
    for{
      dt <- Gen.choose(0, dtMax);
      ts = tMax - dt
    } yield A(ida, ts)
  }

  def genB(ida: Int) = {
    for{
      idb <- Gen.choose(0, idMax);
      dt <- Gen.choose(0, dtMax);
      ts = tMax - dt
    } yield B(idb, ts, ida)
  }

  def genBs(ida: Int,numBs:Int) : Gen[List[B]] = {
    Gen.listOfN(numBs,genB(ida))
  }

  def genABPair(ida: Int, config:CfgCardinality): Gen[(Option[A], Seq[B])] = {
    for (
      k <- Gen.choose(1, if(config.leftOptional) 2 else 1);
      numBs <- Gen.choose(0,config.rightDist.numMax);
      a <- genA(ida);
      b <- genBs(ida,numBs)
    ) yield {
      k match {
        case 1 => (Some(a),b)
        case 2 => (None,b)
      }
    }
  }

  def genABPair(cfg:CfgCardinality) : Gen[(Option[A], Seq[B])] = {
    for {
      ida <- Gen.choose(0, idMax);
      pair <- genABPair(ida, cfg)
    } yield pair
  }

  def genABPairNonempty(cfg:CfgCardinality) = {
    genABPair(cfg) suchThat (pair => !pair._1.isEmpty || !pair._2.isEmpty )
  }

  def genABPairNonemptyNoseq(cfg:CfgCardinality) = {
    genABPairNonempty(cfg) map (xy => (xy._1, xy._2.headOption))
  }
}

object GenJoinInput {
  def print(xy: Seq[(Option[A], Option[B])], tagPP: String) = {
    for {
      ((x, y), i) <- xy.sorted(Ordering[(Boolean,Boolean,Int,Int)].on((a:(Option[A], Option[B])) => (a._1.isDefined,a._2.isDefined,a._1.map(_.id).getOrElse(0),a._2.map(_.ida).getOrElse(0)))).zipWithIndex
    } {
      println(Seq(
        tagPP,
        i,
        x.map(_.ts).getOrElse(""),
        x.map(_.id).getOrElse(""),
        y.map(_.ida).getOrElse(""),
        y.map(_.id).getOrElse(""),
        y.map(_.ts).getOrElse("")
      ).mkString("\t"))
    }
  }
}
