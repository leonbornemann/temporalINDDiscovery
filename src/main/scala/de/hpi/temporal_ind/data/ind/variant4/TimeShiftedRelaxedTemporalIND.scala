package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.ind.TemporalIND

class TimeShiftedRelaxedTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                     rhs: AbstractOrderedColumnHistory[T],
                                                     deltaInNanos:Long,
                                                     maxEpsilonInNanos:Long,
                                                     useWildcardLogic:Boolean) extends TemporalIND(lhs,rhs) {

  override def toString: String =  s"Variant4TemporalIND(${lhs.id},${rhs.id},$deltaInNanos,$maxEpsilonInNanos)"

  var solver:Option[TimeShiftedRelaxedINDDynamicProgrammingSolver[T]] = None

  def getOrCeateSolver() = {
    if (!solver.isDefined)
      solver = Some(new TimeShiftedRelaxedINDDynamicProgrammingSolver(lhs,rhs,deltaInNanos,useWildcardLogic))
    solver.get
  }

  override def isValid: Boolean = {
    val solver = getOrCeateSolver()
    solver.optimalMappingCost <= maxEpsilonInNanos
  }

  def getEpslionOptimizedMapping = {
    val matrix = getOrCeateSolver()
    matrix.optimalMapping
  }

  override def absoluteViolationTime: Long = {
    getOrCeateSolver()
      .optimalMappingCost
  }
}
