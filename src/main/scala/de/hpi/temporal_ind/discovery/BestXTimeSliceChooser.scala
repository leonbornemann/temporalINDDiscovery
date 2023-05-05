package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import scala.util.Random

class BestXTimeSliceChooser(historiesEnriched: ColumnHistoryStorage, expectedQueryParamters: TINDParameters, random: Random) extends TimeSliceChooser {
  override def getNextTimeSlice(): (Instant, Instant) = ???
}
