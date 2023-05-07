package de.hpi.temporal_ind.discovery

import com.google.zetasketch.HyperLogLogPlusPlus
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import java.time.temporal.ChronoUnit

abstract class WeightBasedTimeSliceChooser(historiesEnriched:ColumnHistoryStorage, expectedQueryParamters: TINDParameters) extends TimeSliceChooser(expectedQueryParamters){

  val granularity = ChronoUnit.DAYS
  val hllBuilder = new HyperLogLogPlusPlus.Builder();

  def initTimestampToWeights() = {
    collection.mutable.TreeMap[Instant, HyperLogLogPlusPlus[String]]() ++ GLOBAL_CONFIG
      .ALL_DAYS
      .map(i => (i.truncatedTo(ChronoUnit.DAYS), hllBuilder.buildForStrings()))
      .filter(t => t._1.isAfter(GLOBAL_CONFIG.earliestInstant) && t._1.isBefore(GLOBAL_CONFIG.lastInstant))
  }

  val timestampToWeight = initTimestampToWeights()

  def addAllToSketch(sketch: HyperLogLogPlusPlus[String], values: Set[String]) = values.foreach(v => sketch.add(v))

  //fill sketches:
  historiesEnriched.histories.foreach(h => {
    val withIndex = h.och.history.versions.toIndexedSeq
      .zipWithIndex
    withIndex.map { case (t, i) => {
      val begin = t._1.truncatedTo(ChronoUnit.DAYS)
      val end = if (i == withIndex.size-1) GLOBAL_CONFIG.lastInstant else withIndex(i + 1)._1._1
      (0 until granularity.between(begin, end).toInt)
        .foreach(l => {
          val date = begin.plus(l, granularity)
          if(!timestampToWeight.contains(date))
            println()
          addAllToSketch(timestampToWeight(begin.plus(l, granularity)), t._2.values)
        })
    }
    }
  })
  //transform to weight:
  val weights = timestampToWeight.map(t => (t._1, t._2.result().toInt))

}