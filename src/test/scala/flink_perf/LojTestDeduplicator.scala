package flink_perf

case class LojTestDeduplicator[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                                    idFromX:X=>String, idFromY:Y=>String,
                                    tsFromX:X=>Long, tsFromY:Y=>Long)(xys:Seq[(X,Seq[Y])]) {
  import collection.JavaConverters._
  def get() = {
    val xs : Iterable[X] = xys.map(_._1)
    val ys : Iterable[Y] = xys.flatMap(_._2)
    val xyseq = versionDeduplicator.dedupeLeftOuterSeq[X,Y](keyFromX,keyFromY,idFromX,idFromY,tsFromX,tsFromY,xs.asJava,ys.asJava)
    xyseq
  }
}

case class FojTestDeduplicator[X,Y](keyFromX:X=>String, keyFromY:Y=>String,
                                    idFromX:X=>String, idFromY:Y=>String,
                                    tsFromX:X=>Long, tsFromY:Y=>Long)(xys:Seq[(Seq[X],Seq[Y])]) {
  import collection.JavaConverters._
  def get() = {
    val xs : Iterable[X] = xys.flatMap(_._1)
    val ys : Iterable[Y] = xys.flatMap(_._2)
    versionDeduplicator.dedupeFullOuterSeq[X,Y](keyFromX,keyFromY,idFromX,idFromY,tsFromX,tsFromY,xs.asJava,ys.asJava)
  }
}
