package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.ChronoUnit

object TimeUtil {

  val nanosPerDay = 86400000000000L

  def toRelativeTimeAmount(nanos: Long,totalTime:Long) = {
    (nanos / nanosPerDay) / (totalTime / nanosPerDay).toDouble
  }

  def duration(s:Instant,e:Instant) = {
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
        (t, duration(t,end))
      }
    withDuration
  }

}
