package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.{AbstractOrderedColumnHistory, TimeIntervalSequence}
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant.{FULL_TIME_PERIOD, LHS_INTERSECT_RHS, LHS_ONLY, LHS_UNION_RHS, NormalizationVariant, RHS_ONLY}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.TableFormatter

import java.time.Duration

abstract class TemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T], rhs: AbstractOrderedColumnHistory[T],validationVariant:ValidationVariant.Value) {

  def toCandidateIDs = {
    INDCandidateIDs(lhs.pageID, lhs.tableId, lhs.id, rhs.pageID, rhs.tableId, rhs.id)
  }

  def absoluteViolationScore: Double

  def denominator = {
    validationVariant match {
      case ValidationVariant.LHS_ONLY => lhs.nonEmptyIntervals.summedDurationNanos
      case ValidationVariant.RHS_ONLY => rhs.nonEmptyIntervals.summedDurationNanos
      case ValidationVariant.LHS_UNION_RHS => lhs.nonEmptyIntervals.union(rhs.nonEmptyIntervals).summedDurationNanos
      case ValidationVariant.LHS_INTERSECT_RHS => lhs.nonEmptyIntervals.intersect(rhs.nonEmptyIntervals).summedDurationNanos
      case ValidationVariant.FULL_TIME_PERIOD => GLOBAL_CONFIG.totalTimeInNanos
      case _ => throw new AssertionError(s"Normalization Variant ${validationVariant} not supported")
    }
  }

  def relativeViolationTime() = TimeUtil.toRelativeTimeAmount(absoluteViolationScore.toLong,denominator)

  def isValid:Boolean

  def lhsAndRhsVersionTimestamps = {
    //lhs
    lhs.history.versions.keySet.union(rhs.history.versions.keySet)
  }

  def allIntervals():TimeIntervalSequence = {
    val leftAndRight = lhsAndRhsVersionTimestamps.toIndexedSeq
    val list = if(leftAndRight.contains(GLOBAL_CONFIG.earliestInstant)) leftAndRight else IndexedSeq(GLOBAL_CONFIG.earliestInstant) ++ leftAndRight
    if (list.last == GLOBAL_CONFIG.lastInstant)
      TimeIntervalSequence.fromSortedStartTimes(list.slice(0, list.size - 1), list.last)
    else
      TimeIntervalSequence.fromSortedStartTimes(list, GLOBAL_CONFIG.lastInstant)
  }

  def validationIntervals:TimeIntervalSequence = {
    validationVariant match {
      case LHS_ONLY => lhs.nonEmptyIntervals
      case RHS_ONLY => rhs.nonEmptyIntervals
      case LHS_UNION_RHS => new TimeIntervalSequence(allIntervals()
        .intervals
        .filter(i => !lhs.versionAt(i._1).isDelete || !rhs.versionAt(i._1).isDelete))
      case LHS_INTERSECT_RHS => lhs.nonEmptyIntervals.intersect(rhs.nonEmptyIntervals)
      case FULL_TIME_PERIOD => allIntervals
    }
  }

  def getTabularEventLineageString = {
    val allDates = lhs.history.versions.keySet.union(rhs.history.versions.keySet).toIndexedSeq.sorted
    val cells1 = IndexedSeq(lhs.id) ++ allDates.map(t => lhs.versionAt(t).values.toIndexedSeq.sorted.mkString(","))
    val cells2 = IndexedSeq(rhs.id) ++ allDates.map(t => rhs.versionAt(t).values.toIndexedSeq.sorted.mkString(","))
    val header = Seq(toString + s" -- isValid:$isValid") ++ allDates
    TableFormatter.format(Seq(header) ++ Seq(cells1,cells2))
  }
}
