package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory

import java.io.PrintWriter
import java.time.Instant

class TimeSliceStatisticsExtractor(data: IndexedSeq[OrderedColumnHistory],
                                   allSlices: IndexedSeq[(Instant, Instant)],
                                   resultPR: PrintWriter) extends StrictLogging{

  def extractForAll() = {
    resultPR.println("begin,end,numHistoriesWithVersionPresent,numVersionsPresentSum,approxDistinctValueCount")
    allSlices.foreach(slice => {
      logger.debug(s"Processing Slice $slice")
      val stats = TimeSliceStats()
      data.foreach(och => och.extractStatsForTimeRange(slice,stats))
      resultPR.println(s"${slice._1}.${slice._2},${stats.numHistoriesWithVersionPresent},${stats.numVersionsPresentSum},${stats.hashedDistinctValues.size}")
    })
    resultPR.close()
  }


}
