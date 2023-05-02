package de.hpi.temporal_ind.data.ind
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.ChronoUnit

class ConstantWeightFunction() extends TimestampWeightFunction() {

  override def weight(t: Instant): Double = 1.0

  override def weight(startInclusive: Instant, endExclusive: Instant): Double = ChronoUnit.NANOS.between(startInclusive,endExclusive)//

  //override def summedWeightNanos(startInclusive: Instant, endExclusive: Instant): Double = ChronoUnit.NANOS.between(startInclusive,endExclusive)
}
