package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.{Duration, Instant}

class SimpleTimeWindowTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T], rhs: AbstractOrderedColumnHistory[T], deltaInNanos: Long) extends TemporalIND(lhs,rhs){

  def relativeViolationTime: Double = {
    val violationTime = TimeUtil.withDurations(relevantTimestamps)
      .map{case (t,dur) =>
        val lhsVersion = lhs.versionAt(t).values
        val values = rhs.valuesInWindow(t.minus(Duration.ofNanos(deltaInNanos)),t.plus(Duration.ofNanos(deltaInNanos)))
        val allContained = lhsVersion.forall(v => values.contains(v))
        if(allContained) 0 else dur
      }
      .sum
    TimeUtil.toRelativeTimeAmount(violationTime)
  }

  override def toString: String = s"Variant1TemporalIND(${lhs.id},${rhs.id},$deltaInNanos)"

  def relevantTimestamps: Iterable[Instant] = {
    val duration = Duration.ofNanos(deltaInNanos)
    lhs.history.versions.keySet
      .union(rhs.history.versions.keySet.flatMap(t => Set(t.minus(duration),t,t.plus(duration).plusNanos(1))))
      .filter(t => !t.isAfter(GLOBAL_CONFIG.lastInstant) && !t.isBefore(GLOBAL_CONFIG.earliestInstant))
  }

  override def isValid: Boolean = {
    relativeViolationTime == 0.0
  }
}
