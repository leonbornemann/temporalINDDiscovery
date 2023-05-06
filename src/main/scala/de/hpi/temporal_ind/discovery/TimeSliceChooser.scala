package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.TimestampWeightFunction
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import scala.util.Random

abstract class TimeSliceChooser(expectedQueryParamters:TINDParameters) {

  val availableIntervals = collection.mutable.TreeSet[(Instant,Instant)]()
  availableIntervals.add((GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant))

  def isValid(interval: (Instant, Instant)): Boolean = {
    val (myBegin, myEnd) = interval
    availableIntervals.exists { case (begin, end) => !myBegin.isBefore(begin) && myEnd.isBefore(end) }
  }

  def createIntervalOfWeightedLength(t: Instant, omega: TimestampWeightFunction, weight: Double) = {
    omega.getIntervalOfWeight(t, weight + 1)
  }

  def timestamps:Iterator[Instant]

  def getNextTimeSlice(): (Instant, Instant) = {
    val timestampIterator = timestamps
    var t = timestampIterator.next()
    var interval = createIntervalOfWeightedLength(t, expectedQueryParamters.omega, expectedQueryParamters.absoluteEpsilon)
    while (!isValid(interval)) {
      t = timestampIterator.next()
      interval = createIntervalOfWeightedLength(t, expectedQueryParamters.omega, expectedQueryParamters.absoluteEpsilon)
    }
    removeIntervalFromPool(interval._1, interval._2)
    interval
  }

  def removeIntervalFromPool(begin:Instant,endExclusive:Instant) = {
    val containingInterval = availableIntervals.find(t => !t._1.isAfter(begin) && t._2.isAfter(begin)).get
    val (containingIntervalBegin,containingIntervalEnd) = containingInterval
    assert(!containingIntervalEnd.isBefore(endExclusive))
    availableIntervals.remove(containingInterval)
    availableIntervals.add((containingIntervalBegin,begin))
    if(containingIntervalEnd!=endExclusive){
      assert(containingIntervalEnd.isAfter(endExclusive))
      availableIntervals.add((endExclusive,containingIntervalEnd))
    }
  }
}
object TimeSliceChooser {

  def getChooser(timeSliceChoiceMethod: TimeSliceChoiceMethod.Value,
                 historiesEnriched: ColumnHistoryStorage,
                 expectedQueryParamters:TINDParameters,
                 random:Random) = {
    timeSliceChoiceMethod match {
      case TimeSliceChoiceMethod.RANDOM => new RandomTimeSliceChooser(historiesEnriched,expectedQueryParamters,random)
      case TimeSliceChoiceMethod.WEIGHTED_RANDOM => new WeightedRandomTimeSliceChooser(historiesEnriched,expectedQueryParamters,random)
      case TimeSliceChoiceMethod.BESTX => new BestXTimeSliceChooser(historiesEnriched,expectedQueryParamters,random)
    }
  }

}
