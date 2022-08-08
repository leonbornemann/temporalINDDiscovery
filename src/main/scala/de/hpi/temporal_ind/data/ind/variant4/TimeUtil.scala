package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.ChronoUnit

object TimeUtil {

  def toRelativeTimeAmount(nanos: Long) = {
    val totalTime = GLOBAL_CONFIG.totalTimeInNanos
    (nanos / GLOBAL_CONFIG.nanosPerDay) / (totalTime / GLOBAL_CONFIG.nanosPerDay).toDouble
  }

  def withDurations(timestamps:Iterable[Instant]) = {
    val withIndex = timestamps
      .toIndexedSeq
      .sorted
      .zipWithIndex
    val withDuration = withIndex
      .map { case (t, i) =>
        val end = if (i == withIndex.size - 1) GLOBAL_CONFIG.lastInstant else withIndex(i + 1)._1
        (t, ChronoUnit.NANOS.between(t, end))
      }
    withDuration
  }

}
