package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.OrderedColumnHistory

import java.time.Instant

trait EpsilonCostFunction {

  def cost(lhs: OrderedColumnHistory,
           rhs: OrderedColumnHistory,
           tLHS: Instant,
           tRHSLowerInclusive: Instant,
           tRHSUpperInclusive: Instant): Int
}
