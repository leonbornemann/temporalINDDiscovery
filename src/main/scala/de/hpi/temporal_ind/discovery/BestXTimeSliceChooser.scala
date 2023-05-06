package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import scala.util.Random

class BestXTimeSliceChooser(historiesEnriched: ColumnHistoryStorage, expectedQueryParamters: TINDParameters, random: Random)
  extends WeightBasedTimeSliceChooser(historiesEnriched, expectedQueryParamters) {
  override def timestamps: Iterator[Instant] = {
    weights
      .toIndexedSeq
      .sortBy(_._2)
      .map(_._1)
      .iterator
  }
}
