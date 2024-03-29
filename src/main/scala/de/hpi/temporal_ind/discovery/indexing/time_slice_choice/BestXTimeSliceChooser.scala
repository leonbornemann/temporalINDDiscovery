package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.io.File
import java.time.Instant
import scala.util.Random

class BestXTimeSliceChooser(historiesEnriched: ColumnHistoryStorage, expectedQueryParamters: TINDParameters, random: Random,importFile:File,allowReverseSearch:Boolean)
  extends DistinctValueWeightBasedTimeSliceChooser(historiesEnriched, expectedQueryParamters,allowReverseSearch) {

  val weightsFromFile = if(importFile.exists()) {
    logger.debug(s"Reading pre-computed weights from file $importFile and re-sorting")
    Some(WeightedShuffledTimestamps.fromJsonFile(importFile.getAbsolutePath).shuffled)
  } else if (importFile.getParentFile.exists() && !importFile.getParentFile.listFiles().isEmpty) {
    val importFileOther = importFile.getParentFile.listFiles().head
    logger.debug(s"Reading pre-computed weights from file $importFileOther and re-sorting")
    Some(WeightedShuffledTimestamps.fromJsonFile(importFileOther.getAbsolutePath).shuffled)
  } else {
    None
  }
  override def timestampsWithWeights = {
    val weightsToUse = if(weightsFromFile.isDefined) weightsFromFile.get else weights
    weightsToUse
      .toIndexedSeq
      .sortBy(_._2)
      .map(t => (t._1,t._2.toLong))
      .iterator
    }
}
