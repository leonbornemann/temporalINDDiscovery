package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import com.google.zetasketch.HyperLogLogPlusPlus
import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import java.time.temporal.ChronoUnit

abstract class DistinctValueWeightBasedTimeSliceChooser(val historiesEnriched:ColumnHistoryStorage,
                                                        expectedQueryParamters: TINDParameters,
                                                        allowReverseSearch:Boolean) extends TimeSliceChooser(expectedQueryParamters,allowReverseSearch) with StrictLogging{

  val granularity = ChronoUnit.DAYS
  val hllBuilder = new HyperLogLogPlusPlus.Builder();

  def initTimestampToWeights() = {
    logger.debug("Recomputing weights")
    timestampToWeightVar = Some(collection.mutable.TreeMap[Instant, HyperLogLogPlusPlus[String]]() ++ GLOBAL_CONFIG
      .ALL_DAYS
      .map(i => (i.truncatedTo(ChronoUnit.DAYS), hllBuilder.buildForStrings()))
      .filter(t => t._1.isAfter(GLOBAL_CONFIG.earliestInstant) && t._1.isBefore(GLOBAL_CONFIG.lastInstant)))
    historiesEnriched.histories.foreach(h => {
      val withIndex = h.och.history.versions.toIndexedSeq
        .zipWithIndex
      withIndex.map { case (t, i) => {
        val begin = t._1.truncatedTo(ChronoUnit.DAYS)
        val end = if (i == withIndex.size - 1) GLOBAL_CONFIG.lastInstant else withIndex(i + 1)._1._1
        (0 until granularity.between(begin, end).toInt)
          .foreach(l => {
            val date = begin.plus(l, granularity)
            if (!timestampToWeightVar.get.contains(date))
              println()
            addAllToSketch(timestampToWeightVar.get(begin.plus(l, granularity)), t._2.values)
          })
      }
      }
    })
    logger.debug("Finished Recomputing weights")
  }

  private var timestampToWeightVar:Option[ collection.mutable.TreeMap[Instant, HyperLogLogPlusPlus[String]]] = None

  private def timestampsToWeight = {
    if(timestampToWeightVar.isEmpty) {
      initTimestampToWeights()
    }
    timestampToWeightVar.get
  }
  def addAllToSketch(sketch: HyperLogLogPlusPlus[String], values: Set[String]) = values.foreach(v => sketch.add(v))

  //fill sketches:
  //transform to weight:
  def weights = timestampsToWeight.map(t => (t._1, t._2.result().toInt))

}
