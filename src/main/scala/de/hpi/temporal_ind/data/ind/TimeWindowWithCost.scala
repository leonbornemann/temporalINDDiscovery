package de.hpi.temporal_ind.data.ind

import java.time.Instant

case class TimeWindowWithCost(beginInclusive:Instant, endExclusive:Instant, cost:Double) {

}
