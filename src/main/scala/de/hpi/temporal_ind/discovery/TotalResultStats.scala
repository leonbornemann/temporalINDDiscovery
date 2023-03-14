package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.JsonWritable

case class TotalResultStats(numTimeSliceIndices: Int, dataLoadingTimeMS: Double, requirecValuesIndexBuildTime: Double, summedTimeSliceIndicesBuildTimes: Double, totalIndexQueryTime: Double, totalSubsetValidationTime: Double, totalTemporalValidationTime: Double) extends JsonWritable[TotalResultStats]{


  def toCSV = s"$numTimeSliceIndices,$dataLoadingTimeMS,$requirecValuesIndexBuildTime,$summedTimeSliceIndicesBuildTimes,$totalIndexQueryTime,$totalSubsetValidationTime,$totalTemporalValidationTime"
}
object TotalResultStats {

  def schema = "numTimeSliceIndices,dataLoadingTimeMS,requirecValuesIndexBuildTime,summedTimeSliceIndicesBuildTimes,totalIndexQueryTime,totalSubsetValidationTime,totalTemporalValidationTime"
}
