package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import com.google.zetasketch.HyperLogLogPlusPlus
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.attribute_history.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.{ColumnHistoryStorage, EnrichedColumnHistory}

import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.io.Source
import scala.util.Random

class DynamicWeightedRandomTimeSliceChooser(historiesEnriched: ColumnHistoryStorage,
                                            expectedQueryParamters: TINDParameters,
                                            random: Random,
                                            importFile:File) extends TimeSliceChooser(expectedQueryParamters){
  def exportAsFile(weightedShuffleFile: File) = {
    WeightedShuffledTimestamps(returnedTimestampsWithWeightsInOrder.map(t => (t._1.toString,t._2.toInt)).toIndexedSeq).toJsonFile(weightedShuffleFile)
  }


  val granularity = ChronoUnit.DAYS
  private var timestampToCoveredVersions:Option[ collection.mutable.TreeMap[Instant, collection.mutable.HashSet[Long]]] = None

  def initIterator() = {
    createTimestampsToInitialWeights()
    val shuffler = new DynamicWeightedRandomShuffler(timestampToCoveredVersions.get, random)
    shuffler.map(t => (t._2,t._1))
  }

  val iterator:Iterator[(Instant,Long)] = if(importFile.exists()){
    val list = WeightedShuffledTimestamps.fromJsonFile(importFile.getAbsolutePath)
      .shuffled
    list.map(t => (t._1,t._2.toLong)).iterator
  } else {
    initIterator()
  }



  def createTimestampsToInitialWeights() = {
    timestampToCoveredVersions = Some(collection.mutable.TreeMap[Instant, collection.mutable.HashSet[Long]]() ++ GLOBAL_CONFIG
      .ALL_DAYS
      .map(i => (i.truncatedTo(ChronoUnit.DAYS), collection.mutable.HashSet[Long]()))
      .filter(t => t._1.isAfter(GLOBAL_CONFIG.earliestInstant) && t._1.isBefore(GLOBAL_CONFIG.lastInstant)))
    historiesEnriched.histories.foreach(h => {
      val withIndex = h.och.history.versions.toIndexedSeq
        .zipWithIndex
      withIndex.map { case (t, i) => {
        val begin = t._1.truncatedTo(ChronoUnit.DAYS)
        val end = if (i == withIndex.size - 1) GLOBAL_CONFIG.lastInstant else withIndex(i + 1)._1._1
        (0 until granularity.between(begin, end).toInt)
          .foreach(l => {
            timestampToCoveredVersions.get(begin.plus(l, granularity)).add(t._2.hashCode()) // use hashcode for faster comparisons
          })
      }
      }
    })
  }

  override def timestampsWithWeights:Iterator[(Instant,Long)] = iterator
}
