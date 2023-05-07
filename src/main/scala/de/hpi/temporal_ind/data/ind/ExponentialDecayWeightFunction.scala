package de.hpi.temporal_ind.data.ind
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.Util

import java.time.Instant
import java.time.temporal.ChronoUnit

class ExponentialDecayWeightFunction(a:Double, decayStepUnit:ChronoUnit) extends TimestampWeightFunction() {

  val maxTimestamp = decayStepUnit.between(GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant)

  override def weight(t: Instant): Double = math.pow(a,maxTimestamp-getTimestampAsOrdinal(t))

  def getTimestampAsOrdinal(t: Instant) = maxTimestamp-decayStepUnit.between(t,GLOBAL_CONFIG.lastInstant)

  def getOrdinalAsTimestamp(ordinal:Long) = GLOBAL_CONFIG.earliestInstant.plus(ordinal,decayStepUnit)

  def weightOfInterval(startInclusive: Instant, endExclusive: Instant):Double = {
    val end = getTimestampAsOrdinal(endExclusive)
    val start = getTimestampAsOrdinal(startInclusive)
    val totalWeight = math.pow(a, maxTimestamp) * (math.pow(a, -end) - math.pow(a, -start)) / (1 - a)
    totalWeight / decayStepUnit.between(startInclusive, endExclusive)
  }

  override def weight(startInclusive: Instant, endExclusive: Instant) = ChronoUnit.NANOS.between(startInclusive,endExclusive)*weightOfInterval(startInclusive,endExclusive)

  override def getIntervalOfWeight(start: Instant, weight: Double) = {
    val timestampOfEnd = getEndpointOfInterval(start,weight)
    (start,getOrdinalAsTimestamp(timestampOfEnd))
  }

  def getEndpointOfInterval(start: Instant, weight: Double) = {
    val weightToUse = weight / decayStepUnit.getDuration.toNanos
    val j = -Util.log(a, weightToUse * (1 - a) / Math.pow(a, maxTimestamp) + Math.pow(a,-getTimestampAsOrdinal(start)))
    j.round
  }
}
