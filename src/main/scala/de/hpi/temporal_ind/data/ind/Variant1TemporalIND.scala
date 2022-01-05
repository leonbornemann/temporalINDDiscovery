package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory

import java.time.Duration

class Variant1TemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T], rhs: AbstractOrderedColumnHistory[T], deltaInDays: Int) extends TemporalIND(lhs,rhs){

  override def toString: String = s"Variant1TemporalIND(${lhs.id},${rhs.id},$deltaInDays)"

  override def isValid: Boolean = {
    allRelevantDeltaTimestamps(deltaInDays).forall{case (t) =>
      val lhsVersion = lhs.versionAt(t).values
      val values = rhs.valuesInWindow(t.minus(Duration.ofDays(deltaInDays)),t.plus(Duration.ofDays(deltaInDays)))
      val allContained = lhsVersion.forall(v => values.contains(v))
      allContained
    }
  }
}
