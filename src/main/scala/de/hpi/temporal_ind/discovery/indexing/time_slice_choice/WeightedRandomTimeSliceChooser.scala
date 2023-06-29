package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.io.File
import java.time.Instant
import scala.util.Random

class WeightedRandomTimeSliceChooser(historiesEnriched: ColumnHistoryStorage,
                                     expectedQueryParamters: TINDParameters,
                                     random: Random,
                                     importFile:File,
                                     allowReverseSearch:Boolean) extends DistinctValueWeightBasedTimeSliceChooser(historiesEnriched,expectedQueryParamters,allowReverseSearch) {

  val shuffled = if(importFile.exists()){
    logger.debug(s"Reading pre-computed shuffled weights from file $importFile")
    WeightedShuffledTimestamps.fromJsonFile(importFile.getAbsolutePath)
      .shuffled
  } else if (importFile.getParentFile.exists() && !importFile.getParentFile.listFiles().isEmpty) {
    val importFileOther = importFile.getParentFile.listFiles().head
    logger.debug(s"Reading pre-computed weights from file $importFileOther and re-shuffling them")
    val weightsWronglyShuffled = collection.mutable.TreeMap[Instant,Int]() ++ WeightedShuffledTimestamps.fromJsonFile(importFileOther.getAbsolutePath)
      .shuffled
    val shuffler = new WeightedRandomShuffler(random)
    logger.debug("Begin shuffling ")
    val shuffled = shuffler.shuffle[Instant](weightsWronglyShuffled)
    logger.debug("Finished shuffling ")
    shuffled
  } else {
      val shuffler = new WeightedRandomShuffler(random)
      logger.debug("Begin shuffling ")
      val shuffled = shuffler.shuffle[Instant](weights)
      logger.debug("Finished shuffling ")
      shuffled
    }

//  weights
//    .toIndexedSeq
//    .sortBy(_._1)
//    .foreach(println(_))
//  println()
  def timestampsWithWeights = shuffled.map(t => (t._1,t._2.toLong)).iterator

  def exportAsFile(file:File) = {
    logger.debug("Exporting File")
    WeightedShuffledTimestamps(shuffled.map(t => (t._1.toString,t._2)))
      .toJsonFile(file)
    logger.debug("Done Exporting File")
  }
}
