package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.{Duration, Instant}

class SimpleTimeWindowTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                   rhs: AbstractOrderedColumnHistory[T],
                                                   deltaInNanos: Long,
                                                   useWildcardLogic:Boolean) extends TemporalIND(lhs,rhs){

  def rhsIsWildcardOnlyInRange(lower: Instant, upper: Instant): Boolean = {
    val rhsVersions = rhs.versionsInWindow(lower,upper)
    rhsVersions.size==1 && rhs.versionAt(rhsVersions.head).columnNotPresent
  }

  def relativeViolationTime: Double = {
    val violationTime: Long = absoluteViolationTime
    TimeUtil.toRelativeTimeAmount(violationTime)
  }

  def absoluteViolationTime = {
    val violationTime = TimeUtil.withDurations(relevantTimestamps)
      .map { case (t, dur) =>
        val lhsVersion = lhs.versionAt(t).values
        val lower = t.minus(Duration.ofNanos(deltaInNanos))
        val upper = t.plus(Duration.ofNanos(deltaInNanos))
        val values = rhs.valuesInWindow(lower, upper)
        val allContained = lhsVersion.forall(v => values.contains(v))
        if (allContained || (useWildcardLogic && rhsIsWildcardOnlyInRange(lower, upper))) 0 else dur
      }
      .sum
    violationTime
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
