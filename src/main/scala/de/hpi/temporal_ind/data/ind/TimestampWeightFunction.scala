package de.hpi.temporal_ind.data.ind

import java.time.Instant
import java.time.temporal.ChronoUnit

abstract class TimestampWeightFunction(timeUnit:ChronoUnit) {

  def weight(t:Instant):Double

  def weight(startInclusive:Instant,endExclusive:Instant):Double

  def summedWeightNanos(startInclusive: Instant, endExclusive: Instant):Double
}
