package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.{ChronoUnit, Temporal}

class SimpleRelaxedTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T], rhs: AbstractOrderedColumnHistory[T],maxEpsilonInNanos:Long) extends TemporalIND[T](lhs,rhs){

  def relativeViolationTime = {
    TimeUtil.toRelativeTimeAmount(summedViolationTime)
  }

  override def toString: String = s"SimpleRelaxedTemporalIND(${lhs.id},${rhs.id})"

  override def isValid:Boolean = {
    val violationTimeNanos: Long = summedViolationTime
    violationTimeNanos <= maxEpsilonInNanos
  }

  def summedViolationTime = {
    val withDuration: IndexedSeq[(Instant, Long)] = TimeUtil.withDurations(lhsAndRhsVersionTimestamps)
    val violationTimeNanos = withDuration.map { case (t, dur) =>
      val lhsAtT = lhs.versionAt(t)
      val rhsAtT = rhs.versionAt(t)
      val isValid = lhsAtT.values.forall(v => rhsAtT.values.contains(v))
      if (isValid) 0 else dur
    }
      .sum
    violationTimeNanos
  }

}
