package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.ind.TemporalIND

class Variant4TemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                           rhs: AbstractOrderedColumnHistory[T],
                          deltaInDays:Int,
                          maxEpsilon:Int,
                          costFunction:EpsilonCostFunction) extends TemporalIND(lhs,rhs) {

  override def toString: String =  s"Variant4TemporalIND(${lhs.id},${rhs.id},$deltaInDays,$maxEpsilon)"

  var solver:Option[Variant4DynamicProgrammingSolver[T]] = None

  def getOrCeateSolver() = {
    if (!solver.isDefined)
      solver = Some(new Variant4DynamicProgrammingSolver(lhs,rhs,deltaInDays,costFunction))
    solver.get
  }

  override def isValid: Boolean = {
    val matrix = getOrCeateSolver()
    matrix.bestMappingCost <= maxEpsilon
  }



  def getEpslionOptimizedMapping = {
    val matrix = getOrCeateSolver()
    matrix.getOptimalMappingFunction
  }
}
