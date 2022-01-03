package de.hpi.temporal_ind.data.ind.variant4
import de.hpi.temporal_ind.data.column.OrderedColumnHistory

import java.time.Instant

class Variant4_2_CostFunction extends EpsilonCostFunction {

  override def cost(lhs: OrderedColumnHistory,
                    rhs: OrderedColumnHistory,
                    tLHS: Instant,
                    tRHSLowerInclusive: Instant,
                    tRHSUpperInclusive: Instant): Int = {
    val toCover = lhs.versionAt(tLHS).values
    val inRHS = rhs.valuesInWindow(tRHSLowerInclusive,tRHSUpperInclusive)
    toCover.filter(e => !inRHS.contains(e)).size
  }
}
