package de.hpi.temporal_ind.data.ind
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.ChronoUnit

class ConstantWeightFunction(timeUnit:ChronoUnit) extends TimestampWeightFunction(timeUnit:ChronoUnit) {

  override def weight(t: Instant): Double = 1.0

  override def weight(startInclusive: Instant, endExclusive: Instant): Double = timeUnit.between(startInclusive,endExclusive)
}
