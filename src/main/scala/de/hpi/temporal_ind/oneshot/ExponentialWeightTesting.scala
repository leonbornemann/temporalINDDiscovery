package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.ind.ExponentialDecayWeightFunction
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import java.time.temporal.ChronoUnit

object ExponentialWeightTesting extends App {


  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")

  val units = Seq(ChronoUnit.NANOS,ChronoUnit.DAYS)
  for(unit <- units){
    GLOBAL_CONFIG.earliestInstant = Instant.parse("2001-01-01T00:00:00Z")
    GLOBAL_CONFIG.lastInstant = GLOBAL_CONFIG.earliestInstant.plus(100, unit)
    val exponentialFunction = new ExponentialDecayWeightFunction(0.9, unit)
    val begin = GLOBAL_CONFIG.earliestInstant
    println(exponentialFunction.getTimestampAsOrdinal(begin))
    (1 to 100).foreach(i => {
      val weightOfInterval = exponentialFunction.weight(begin, begin.plus(i, unit))
      val res = exponentialFunction.getEndpointOfInterval(begin, weightOfInterval)
      if (!(res.round.toLong == i)) {
        println(" not fine")
      }
      exponentialFunction.getIntervalOfWeight(begin,weightOfInterval)
      //println(i,exponentialFunction.weight(begin,begin.plusNanos(i)))
    })
  }


}
