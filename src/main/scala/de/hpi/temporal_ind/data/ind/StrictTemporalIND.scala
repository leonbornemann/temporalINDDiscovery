package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.attribute_history.data.original.{ColumnHistory, OrderedColumnHistory}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil

import java.time.Duration
import java.time.temporal.{ChronoUnit, TemporalUnit}

class StrictTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                         rhs: AbstractOrderedColumnHistory[T],
                                         wildcardLogic:Boolean,
                                         validationPeriod:ValidationVariant.Value) extends TemporalIND[T](lhs,rhs,validationPeriod){

  override def toString: String = s"StrictTemporalIND(${lhs.id},${rhs.id})"

  override def isValid:Boolean = {
    validationIntervals.intervals.forall{case (s,e) =>
      val lhsAtT = lhs.versionAt(s)
      val rhsAtT = rhs.versionAt(s)
      (rhsAtT.isDelete && wildcardLogic) || lhsAtT.values.forall(v => rhsAtT.values.contains(v))
    }
  }

  def displayVersionTable = {

  }

  /***
   * the total amount of time (in days) during which at least one of the histories was non-empty
   */
  def totalActiveTimeInDays = {
    val nonEmptyIntervalsLeft = lhs.nonEmptyIntervals
    val nonEmptyIntervalsRight = rhs.nonEmptyIntervals
    nonEmptyIntervalsLeft.union(nonEmptyIntervalsRight).summedDurationNanos / TimeUtil.nanosPerDay
  }

  def overlapTimeInDays = {
    val nonEmptyIntervalsLeft = lhs.nonEmptyIntervals
    val nonEmptyIntervalsRight = rhs.nonEmptyIntervals
    nonEmptyIntervalsLeft.intersect(nonEmptyIntervalsRight).summedDurationNanos / TimeUtil.nanosPerDay
  }

  def nonOverlapTimeInDays = {
    val nonEmptyIntervalsLeft = lhs.nonEmptyIntervals
    val nonEmptyIntervalsRight = rhs.nonEmptyIntervals
    nonEmptyIntervalsLeft.unionOfDiffs(nonEmptyIntervalsRight).summedDurationNanos / TimeUtil.nanosPerDay

  }

  override def absoluteViolationScore: Double = if(isValid) 0 else GLOBAL_CONFIG.totalTimeInNanos
}
