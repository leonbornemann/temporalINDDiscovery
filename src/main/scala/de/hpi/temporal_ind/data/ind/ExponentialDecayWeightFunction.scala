package de.hpi.temporal_ind.data.ind
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.ChronoUnit

class ExponentialDecayWeightFunction(a:Double,timeUnit:ChronoUnit) extends TimestampWeightFunction() {

  val maxTimestamp = timeUnit.between(GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant)

  override def weight(t: Instant): Double = math.pow(a,maxTimestamp-getTimestamp(t))

  def getTimestamp(t: Instant) = maxTimestamp-timeUnit.between(t,GLOBAL_CONFIG.lastInstant)

  def weightOfInterval(startInclusive: Instant, endExclusive: Instant):Double = {
    val end = getTimestamp(endExclusive)
    val start = getTimestamp(startInclusive)
    (math.pow(a, maxTimestamp) * (math.pow(a, -end) - math.pow(a, -start)) / (1 - a)) / timeUnit.between(startInclusive, endExclusive)
  }

  override def weight(startInclusive: Instant, endExclusive: Instant) = ChronoUnit.NANOS.between(startInclusive,endExclusive)*weightOfInterval(startInclusive,endExclusive)

  override def getIntervalOfWeight(start: Instant, weight: Double) = {
    ???
  }
}
