package flink_perf

case class LojTestDeduplicator[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                                    idFromX:X=>String, idFromY:Y=>String,
                                    tsFromX:X=>Long, tsFromY:Y=>Long)(xys:Seq[(X,Seq[Y])]) {
  import collection.JavaConverters._
  def get() = {
    val xs : Iterable[X] = xys.map(_._1)
    val ys : Iterable[Y] = xys.flatMap(_._2)
    val xyseq = flink_perf.joins.dedupeLeftOuterSeq[X,Y](keyFromX,keyFromY,idFromX,idFromY,tsFromX,tsFromY,xs.asJava,ys.asJava)
    xyseq.flatMap(v => if(v._1.isEmpty) Seq() else Seq((v._1.get, v._2)))
  }
}
