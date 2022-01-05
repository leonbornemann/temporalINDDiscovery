package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.{ColumnHistory, OrderedColumnHistory}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Duration
import java.time.temporal.{ChronoUnit, TemporalUnit}

class StrictTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T], rhs: AbstractOrderedColumnHistory[T]) extends TemporalIND[T](lhs,rhs){

  override def toString: String = s"StrictTemporalIND(${lhs.id},${rhs.id})"

  override def isValid:Boolean = {
    allRelevantTimestamps.forall(t => {
      val lhsAtT = lhs.versionAt(t)
      val rhsAtT = rhs.versionAt(t)
      lhsAtT.values.forall(v => rhsAtT.values.contains(v))
    })
  }

  def displayVersionTable = {

  }

  /***
   * the total amount of time (in days) during which at least one of the histories was non-empty
   */
  def totalActiveTimeInDays = {
    val nonEmptyIntervalsLeft = lhs.nonEmptyIntervals
    val nonEmptyIntervalsRight = rhs.nonEmptyIntervals
    nonEmptyIntervalsLeft.union(nonEmptyIntervalsRight).summedDuration.getSeconds /(60*60*24)
  }

  def overlapTimeInDays = {
    val nonEmptyIntervalsLeft = lhs.nonEmptyIntervals
    val nonEmptyIntervalsRight = rhs.nonEmptyIntervals
    nonEmptyIntervalsLeft.intersect(nonEmptyIntervalsRight).summedDuration.getSeconds /(60*60*24)
  }

  def nonOverlapTimeInDays = {
    val nonEmptyIntervalsLeft = lhs.nonEmptyIntervals
    val nonEmptyIntervalsRight = rhs.nonEmptyIntervals
    nonEmptyIntervalsLeft.unionOfDiffs(nonEmptyIntervalsRight).summedDuration.getSeconds /(60*60*24)

  }
}
