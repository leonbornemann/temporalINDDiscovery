package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.{ColumnHistory, OrderedColumnHistory}

class StrictTemporalIND(lhs: OrderedColumnHistory, rhs: OrderedColumnHistory) extends TemporalIND(lhs,rhs){

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

}
