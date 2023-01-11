package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant.NormalizationVariant
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.TableFormatter

import java.time.Duration

abstract class TemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T], rhs: AbstractOrderedColumnHistory[T]) {

  def absoluteViolationTime: Long

  def relativeViolationTime(normalizationVariant: ValidationVariant.Value) = {
    normalizationVariant match {
      case ValidationVariant.LHS_ONLY => TimeUtil.toRelativeTimeAmount(absoluteViolationTime,lhs.nonEmptyIntervals.summedDurationNanos)
      case ValidationVariant.RHS_ONLY => TimeUtil.toRelativeTimeAmount(absoluteViolationTime,rhs.nonEmptyIntervals.summedDurationNanos)
      case ValidationVariant.LHS_UNION_RHS => TimeUtil.toRelativeTimeAmount(absoluteViolationTime,lhs.nonEmptyIntervals.union(rhs.nonEmptyIntervals).summedDurationNanos)
      case ValidationVariant.LHS_INTERSECT_RHS => TimeUtil.toRelativeTimeAmount(absoluteViolationTime,lhs.nonEmptyIntervals.intersect(rhs.nonEmptyIntervals).summedDurationNanos)
      case ValidationVariant.FULL_TIME_PERIOD => TimeUtil.toRelativeTimeAmount(absoluteViolationTime,GLOBAL_CONFIG.totalTimeInNanos)
      case _ => throw new AssertionError(s"Normalization Variant ${normalizationVariant} not supported")
    }
  }

  def isValid:Boolean

  def lhsAndRhsVersionTimestamps = {
    //lhs
    lhs.history.versions.keySet.union(rhs.history.versions.keySet)
  }

  def getTabularEventLineageString = {
    val allDates = lhs.history.versions.keySet.union(rhs.history.versions.keySet).toIndexedSeq.sorted
    val cells1 = IndexedSeq(lhs.id) ++ allDates.map(t => lhs.versionAt(t).values.toIndexedSeq.sorted.mkString(","))
    val cells2 = IndexedSeq(rhs.id) ++ allDates.map(t => rhs.versionAt(t).values.toIndexedSeq.sorted.mkString(","))
    val header = Seq(toString + s" -- isValid:$isValid") ++ allDates
    TableFormatter.format(Seq(header) ++ Seq(cells1,cells2))
  }
}
