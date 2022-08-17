package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.{ChronoUnit, Temporal}

class SimpleRelaxedTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                rhs: AbstractOrderedColumnHistory[T],
                                                maxEpsilonInNanos:Long,
                                                useWildcardLogic:Boolean) extends TemporalIND[T](lhs,rhs){

  override def toString: String = s"SimpleRelaxedTemporalIND(${lhs.id},${rhs.id})"

  override def isValid:Boolean = {
    val violationTimeNanos: Long = absoluteViolationTime
    violationTimeNanos <= maxEpsilonInNanos
  }

  def absoluteViolationTime = {
    val withDuration: IndexedSeq[(Instant, Long)] = TimeUtil.withDurations(lhsAndRhsVersionTimestamps)
    val violationTimeNanos = withDuration.map { case (t, dur) =>
      val lhsAtT = lhs.versionAt(t)
      val rhsAtT = rhs.versionAt(t)
      val isValid = lhsAtT.values.forall(v => rhsAtT.values.contains(v)) || (rhsAtT.isDelete && useWildcardLogic)
      if (isValid) 0 else dur
    }
      .sum
    violationTimeNanos
  }

}
