package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.TimestampWeightFunction
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import scala.util.Random

class RandomTimeSliceChooser(historiesEnriched: ColumnHistoryStorage,
                             expectedQueryParamters:TINDParameters,
                             random:Random) extends TimeSliceChooser{


  val timestamps = random.shuffle(GLOBAL_CONFIG.ALL_DAYS).iterator

  def isValid(interval: (Instant,Instant)): Boolean = {
    val (myBegin,myEnd) = interval
    availableIntervals.exists {case (begin,end) => !myBegin.isBefore(begin) && myEnd.isBefore(end)}
  }

  def createIntervalOfWeightedLength(t: Instant, omega: TimestampWeightFunction, weight: Double) = {
    omega.getIntervalOfWeight(t,weight+1)
  }

  def getNextTimeSlice():(Instant,Instant) = {
    var t = timestamps.next()
    var interval = createIntervalOfWeightedLength(t,expectedQueryParamters.omega,expectedQueryParamters.absoluteEpsilon)
    while(!isValid(interval)){
      t = timestamps.next()
      interval = createIntervalOfWeightedLength(t,expectedQueryParamters.omega,expectedQueryParamters.absoluteEpsilon)
    }
    removeIntervalFromPool(interval._1,interval._2)
    interval
  }

}
