package flink_perf

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator, StreamSourceContexts}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.slf4j.LoggerFactory

case class StreamMonitor[T](idleTimeout: Long)
  extends AbstractStreamOperator[T]
    with OneInputStreamOperator[T, T] {
  private var sourceCtx: SourceContext[T] = _
  protected def sourceContext: SourceContext[T] = {
    if (sourceCtx == null) {
      val timeCharacteristic = getOperatorConfig.getTimeCharacteristic
      val watermarkInterval  = getRuntimeContext.getExecutionConfig.getAutoWatermarkInterval
      val streamStatus       = getContainingTask.getStreamStatusMaintainer
      sourceCtx = StreamSourceContexts.getSourceContext(
        timeCharacteristic,
        getContainingTask.getProcessingTimeService,
        getContainingTask.getCheckpointLock,
        streamStatus,
        output,
        watermarkInterval,
        idleTimeout
      )
    }
    sourceCtx
  }
  override def processElement(element: StreamRecord[T]): Unit = {
    println(s"${this.getOperatorName}: processing ${element.getValue}")
    sourceContext.collect(element.getValue)
  }

  override def processWatermark1(mark: Watermark): Unit = {
    println(s"${this.getOperatorName} watermark in")
    super.processWatermark1(mark)
  }

  override def processWatermark(mark: Watermark): Unit = {
    println(s"${this.getOperatorName} watermark out t=${mark.getTimestamp}")
    super.processWatermark(mark)
  }
}

