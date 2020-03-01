package flink_perf

import org.scalacheck.Gen
import org.scalacheck.rng.Seed

import scala.collection.mutable.ArrayBuffer

object GenUtil {
  def sampleExactlyN[T](gen2:Gen[T], seedStart:Seed, numSamples:Int) = {
    var seed = seedStart
    var xys = ArrayBuffer[T]()
    while (xys.length < numSamples) {
      val res = gen2.doPureApply(Gen.Parameters.default, seed)
      seed = res.seed
      res.retrieve match {
        case Some(xy) => xys.append(xy)
        case None =>
      }
    }
    xys.toList
  }
}
