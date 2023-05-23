package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import scala.util.Random

class RandomTimeSliceChooser(historiesEnriched: ColumnHistoryStorage,
                             expectedQueryParamters:TINDParameters,
                             random:Random) extends TimeSliceChooser(expectedQueryParamters){


  def timestampsWithWeights = random.shuffle(GLOBAL_CONFIG.ALL_DAYS).iterator.map(t => (t,0))


}
