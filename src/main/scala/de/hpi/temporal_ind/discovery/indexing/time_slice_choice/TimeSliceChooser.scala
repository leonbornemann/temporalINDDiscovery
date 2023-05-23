package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.ind.weight_functions.TimestampWeightFunction
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.io.File
import java.time.Instant
import scala.util.Random

abstract class TimeSliceChooser(expectedQueryParamters:TINDParameters) {

  val availableIntervals = collection.mutable.TreeSet[(Instant,Instant)]()
  availableIntervals.add((GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant))
  var returnedTimestampsWithWeightsInOrder = collection.mutable.ArrayBuffer[(Instant,Long)]()

  def isValid(interval: (Instant, Instant)): Boolean = {
    val (myBegin, myEnd) = interval
    availableIntervals.exists { case (begin, end) => !myBegin.isBefore(begin) && myEnd.isBefore(end) }
  }

  def createIntervalOfWeightedLength(t: Instant, omega: TimestampWeightFunction, weight: Double) = {
    omega.getIntervalOfWeight(t, weight + 1)
  }

  def timestampsWithWeights:Iterator[(Instant,Long)]

  def getNextTimeSlice(): (Instant, Instant) = {
    val timestampIterator = timestampsWithWeights
    val tuple = timestampIterator.next()
    var t = tuple._1
    var weight = tuple._2
    var interval = createIntervalOfWeightedLength(t, expectedQueryParamters.omega, expectedQueryParamters.absoluteEpsilon)
    while (!isValid(interval)) {
      val tuple = timestampIterator.next()
      t = tuple._1
      weight = tuple._2
      interval = createIntervalOfWeightedLength(t, expectedQueryParamters.omega, expectedQueryParamters.absoluteEpsilon)
    }
    removeIntervalFromPool(interval._1, interval._2)
    returnedTimestampsWithWeightsInOrder.addOne((t,weight))
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
                 random:Random,
                 shuffledFile:File) = {
    timeSliceChoiceMethod match {
      case TimeSliceChoiceMethod.RANDOM => new RandomTimeSliceChooser(historiesEnriched,expectedQueryParamters,random)
      case TimeSliceChoiceMethod.WEIGHTED_RANDOM => new WeightedRandomTimeSliceChooser(historiesEnriched,expectedQueryParamters,random,shuffledFile)
      case TimeSliceChoiceMethod.BESTX => new BestXTimeSliceChooser(historiesEnriched,expectedQueryParamters,random,shuffledFile)
      case TimeSliceChoiceMethod.DYNAMIC_WEIGHTED_RANDOM => new DynamicWeightedRandomTimeSliceChooser(historiesEnriched, expectedQueryParamters, random,shuffledFile)
    }
  }

}
