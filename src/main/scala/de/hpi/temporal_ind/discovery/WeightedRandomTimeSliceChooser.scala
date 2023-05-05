package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import scala.util.Random

class WeightedRandomTimeSliceChooser(historiesEnriched: ColumnHistoryStorage, expectedQueryParamters: TINDParameters, random: Random) extends TimeSliceChooser {

//  def getTimeSlices(historiesEnriched: ColumnHistoryStorage): IndexedSeq[(Instant, Instant)] = {
//    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(expectedQueryParameters)
//    if (timeSliceChoiceMethod == TimeSliceChoiceMethod.RANDOM) {
//      random.shuffle(allSlices)
//    } else {
//      val timeSliceToOccurrences = collection.mutable.TreeMap[Instant, (Instant, SimpleCounter)]() ++ allSlices.map(s => (s._1, (s._2, SimpleCounter())))
//      historiesEnriched
//        .histories
//        .foreach(e => e.och.addPresenceForTimeRanges(timeSliceToOccurrences))
//      if (timeSliceChoiceMethod == TimeSliceChoiceMethod.BESTX) {
//        //using simple mutable counter is more efficient than immutable int since we don't haven to reassign in the map
//        timeSliceToOccurrences
//          .toIndexedSeq
//          .sortBy(_._2._2.count)
//          .map(t => (t._1, t._2._1))
//      } else {
//        //do weighted random selection
//        new WeightedRandomShuffler(random).shuffle(timeSliceToOccurrences.map(t => ((t._1, t._2._1), t._2._2.count)))
//      }
//    }
//  }

  override def getNextTimeSlice(): (Instant, Instant) = ???
}
