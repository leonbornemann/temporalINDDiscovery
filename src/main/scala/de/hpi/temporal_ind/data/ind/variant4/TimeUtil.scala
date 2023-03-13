package de.hpi.temporal_ind.data.ind.variant4

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.ChronoUnit

object TimeUtil extends StrictLogging{
  def printExecutionTimeInMS[R](block: => R,taskName:String) = {
    val (res,time) = executionTimeInMS(block)
    logRuntime(time,"MS",taskName)
    res
  }

  def logRuntime(time: Double, unit: String, task: String) = {
    logger.debug(s"Took $time $unit time for Task $task")
  }

  def executionTimeInMS[R](block: => R): (R,Double) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val measuredTime = (t1 - t0) / 1000000.0
    (result,measuredTime)
  }


  val nanosPerDay = 86400000000000L

  def toRelativeTimeAmount(nanos: Long,totalTime:Long) = {
    (nanos / nanosPerDay) / (totalTime / nanosPerDay).toDouble
  }

  def durationNanos(s:Instant, e:Instant) = {
    ChronoUnit.NANOS.between(s, e)
  }

  def withDurations(timestamps:Iterable[Instant]) = {
    val withIndex = timestamps
      .toIndexedSeq
      .sorted
      .zipWithIndex
    val withDuration = withIndex
      .map { case (t, i) =>
        val end = if (i == withIndex.size - 1) GLOBAL_CONFIG.lastInstant else withIndex(i + 1)._1
        (t, durationNanos(t,end))
      }
    withDuration
  }

}
