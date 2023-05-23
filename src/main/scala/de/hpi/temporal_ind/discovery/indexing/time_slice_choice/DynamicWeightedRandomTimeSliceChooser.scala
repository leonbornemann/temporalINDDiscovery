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


  def initIterator() = {
    val shuffler = new WeightedRandomChooser(historiesEnriched, random,t => createIntervalOfWeightedLength(t,expectedQueryParamters.omega,expectedQueryParamters.absoluteEpsilon))
    shuffler.map(t => (t._2,t._1))
  }

  val iterator:Iterator[(Instant,Long)] = if(importFile.exists()){
    val list = WeightedShuffledTimestamps.fromJsonFile(importFile.getAbsolutePath)
      .shuffled
    list.map(t => (t._1,t._2.toLong)).iterator
  } else {
    initIterator()
  }



  override def timestampsWithWeights:Iterator[(Instant,Long)] = iterator
}
