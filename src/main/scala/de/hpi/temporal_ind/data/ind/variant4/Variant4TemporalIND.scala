package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.OrderedColumnHistory
import de.hpi.temporal_ind.data.ind.TemporalIND

import java.time.Instant

class Variant4TemporalIND(lhs:OrderedColumnHistory,
                          rhs:OrderedColumnHistory,
                          deltaInDays:Int,
                          maxEpsilon:Int,
                          costFunction:EpsilonCostFunction) extends TemporalIND(lhs,rhs) {

  override def toString: String =  s"Variant4TemporalIND(${lhs.id},${rhs.id},$deltaInDays,$maxEpsilon)"

  var solver:Option[Variant4DynamicProgrammingSolver] = None

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
