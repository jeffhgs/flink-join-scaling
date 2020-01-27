package flink_perf

import scala.collection.mutable
class OmnicientDeduplicator[X](xs:Seq[X], ktFromX:(X => (String,Long))) {
  val mp = mutable.Map[String,(Long,X)]()
  def get() = {
    for(x <- xs) {
      val (k,t) = ktFromX(x)
      mp.get(k) match {
        case Some((t0,x0)) =>
          if(t > t0) {
            mp.remove(k)
            mp.put(k, (t, x))
          }
        case None =>
          mp.put(k,(t,x))
      }
    }
    mp.toSeq.sortBy(_._1).map(_._2._2)
  }
}
