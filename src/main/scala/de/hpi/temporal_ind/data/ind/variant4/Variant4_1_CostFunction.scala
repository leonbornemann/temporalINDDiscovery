package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.OrderedColumnHistory

import java.time.Instant

class Variant4_1_CostFunction extends EpsilonCostFunction {

  override def cost(lhs: OrderedColumnHistory,
                    rhs: OrderedColumnHistory,
                    tLHS: Instant,
                    tRHSLowerInclusive: Instant,
                    tRHSUpperInclusive: Instant): Int = {
    val toCover = lhs.versionAt(tLHS).values
    val inRHS = rhs.valuesInWindow(tRHSLowerInclusive,tRHSUpperInclusive)
    if (toCover.forall(e => inRHS.contains(e))) 0 else 1
  }
}
