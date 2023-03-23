package de.hpi.temporal_ind.discovery

import java.time.Instant

case class TimeSliceIndexTuningStatRow(queryNum: Int, begin: Instant, end: Instant, countAfterRequiredValues: Int, countAfterTimeSlice: Int) {
  def toCSVLine = s"$queryNum,$begin,$end,$countAfterRequiredValues,$countAfterTimeSlice"

}
object TimeSliceIndexTuningStatRow{
  def schema = "queryNum,begin,end,countAfterRequiredValues,countAfterTimeSlice"
}
