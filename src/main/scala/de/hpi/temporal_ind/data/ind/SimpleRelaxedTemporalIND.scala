package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil

import java.time.Instant
import java.time.temporal.{ChronoUnit, Temporal}

class SimpleRelaxedTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                rhs: AbstractOrderedColumnHistory[T],
                                                maxEpsilonInNanos:Long,
                                                useWildcardLogic:Boolean,
                                                validationVariant:ValidationVariant.Value) extends TemporalIND[T](lhs,rhs,validationVariant){

  override def toString: String = s"SimpleRelaxedTemporalIND(${lhs.id},${rhs.id})"

  override def isValid:Boolean = {
    val violationTimeNanos: Long = absoluteViolationScore.toLong
    violationTimeNanos <= maxEpsilonInNanos
  }

  def absoluteViolationScore = {
    val violationTimeNanos = validationIntervals.intervals.map { case (s, e) =>
      val dur = TimeUtil.durationNanos(s,e)
      val lhsAtT = lhs.versionAt(s)
      val rhsAtT = rhs.versionAt(s)
      val isValid = lhsAtT.values.forall(v => rhsAtT.values.contains(v)) || (rhsAtT.isDelete && useWildcardLogic)
      if (isValid) 0 else dur
    }
      .sum
    violationTimeNanos
  }

}
