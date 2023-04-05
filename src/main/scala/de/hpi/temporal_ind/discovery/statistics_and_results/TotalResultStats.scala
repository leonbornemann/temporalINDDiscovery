package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.JsonWritable
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod

case class TotalResultStats(version:String,
                            sampleSize:Int,
                            bloomFilterSize:Int,
                            violationTrackingEnabled:Boolean,
                            timeSliceChoiceMethod: TimeSliceChoiceMethod.Value,
                            numTimeSliceIndices: Int,
                            dataLoadingTimeMS: Double,
                            requirecValuesIndexBuildTime: Double,
                            summedTimeSliceIndicesBuildTimes: Double,
                            totalIndexQueryTime: Double,
                            totalSubsetValidationTime: Double,
                            totalTemporalValidationTime: Double) extends JsonWritable[TotalResultStats] {


  def toCSV = s"$version,$sampleSize,$bloomFilterSize,$violationTrackingEnabled,$timeSliceChoiceMethod,$numTimeSliceIndices,$dataLoadingTimeMS,$requirecValuesIndexBuildTime,$summedTimeSliceIndicesBuildTimes,$totalIndexQueryTime,$totalSubsetValidationTime,$totalTemporalValidationTime"
}

object TotalResultStats {

  def schema = "version,sampleSize,bloomFilterSize,violationTrackingEnabled,timeSliceChoiceMethod,numTimeSliceIndices,dataLoadingTimeMS,requirecValuesIndexBuildTime,summedTimeSliceIndicesBuildTimes,totalIndexQueryTime,totalSubsetValidationTime,totalTemporalValidationTime"
}
