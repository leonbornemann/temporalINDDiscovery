package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory

import java.time.Instant

class Variant4_2_CostFunction extends EpsilonCostFunction {

  def cost[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                            rhs: AbstractOrderedColumnHistory[T],
                            tLHS: Instant,
                    tRHSLowerInclusive: Instant,
                    tRHSUpperInclusive: Instant): Int = {
    val toCover = lhs.versionAt(tLHS).values
    val inRHS = rhs.valuesInWindow(tRHSLowerInclusive,tRHSUpperInclusive)
    toCover.filter(e => !inRHS.contains(e)).size
  }
}
