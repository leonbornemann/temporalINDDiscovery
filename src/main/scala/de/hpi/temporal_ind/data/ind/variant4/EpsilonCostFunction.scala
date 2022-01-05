package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory

import java.time.Instant

trait EpsilonCostFunction {

  def cost[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                            rhs: AbstractOrderedColumnHistory[T],
                            tLHS: Instant,
                            tRHSLowerInclusive: Instant,
                            tRHSUpperInclusive: Instant): Int
}
