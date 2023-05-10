package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.io.File
import java.time.Instant
import scala.util.Random

class WeightedRandomTimeSliceChooser(historiesEnriched: ColumnHistoryStorage,
                                     expectedQueryParamters: TINDParameters,
                                     random: Random,
                                     importFile:File) extends WeightBasedTimeSliceChooser(historiesEnriched,expectedQueryParamters) {

  val shuffled = if(importFile.exists()){
    logger.debug(s"Reading pre-computed shuffled weights from file $importFile")
    WeightedShuffledTimestamps.fromJsonFile(importFile.getAbsolutePath)
      .shuffled
      .map(_._1)
  } else if (importFile.getParentFile.exists() && !importFile.getParentFile.listFiles().isEmpty) {
    val importFileOther = importFile.getParentFile.listFiles().head
    logger.debug(s"Reading pre-computed weights from file $importFileOther and re-shuffling them")
    val weightsWronglyShuffled = collection.mutable.TreeMap[Instant,Int]() ++ WeightedShuffledTimestamps.fromJsonFile(importFileOther.getAbsolutePath)
      .shuffled
    val shuffler = new WeightedRandomShuffler(random)
    logger.debug("Begin shuffling ")
    val shuffled = shuffler.shuffle[Instant](weightsWronglyShuffled.toIndexedSeq)
    logger.debug("Finished shuffling ")
    shuffled
  } else {
      val shuffler = new WeightedRandomShuffler(random)
      logger.debug("Begin shuffling ")
      val shuffled = shuffler.shuffle[Instant](weights.toIndexedSeq)
      logger.debug("Finished shuffling ")
      shuffled
    }

//  weights
//    .toIndexedSeq
//    .sortBy(_._1)
//    .foreach(println(_))
//  println()
  def timestamps = shuffled.iterator

  def exportAsFile(file:File) = {
    logger.debug("Exporting File")
    val weightMap = weights
    WeightedShuffledTimestamps(shuffled.map(t => (t.toString,weightMap(t))))
      .toJsonFile(file)
    logger.debug("Done Exporting File")
  }
}
