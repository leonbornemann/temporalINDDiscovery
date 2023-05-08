package de.hpi.temporal_ind.data.ind.weight_functions

import java.time.Instant

abstract class TimestampWeightFunction() {
  def getIntervalOfWeight(start: Instant, weight: Double):(Instant, Instant)


  def weight(t:Instant):Double

  def weight(startInclusive:Instant,endExclusive:Instant):Double

  //def summedWeightNanos(startInclusive: Instant, endExclusive: Instant):Double
}
