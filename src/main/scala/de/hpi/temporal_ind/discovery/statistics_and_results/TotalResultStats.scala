package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.JsonWritable
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod

case class TotalResultStats(version:String,
                            indexParameters:TINDParameters,
                            queryParameters:TINDParameters,
                            seed:Long,
                            queryFileName:String,
                            sampleSize:Int,
                            bloomFilterSize:Int,
                            timeSliceChoiceMethod: TimeSliceChoiceMethod.Value,
                            numTimeSliceIndices: Int,
                            dataLoadingTimeMS: Double,
                            requirecValuesIndexBuildTime: Double,
                            summedTimeSliceIndicesBuildTimes: Double,
                            totalIndexQueryTime: Double,
                            totalSubsetValidationTime: Double,
                            totalTemporalValidationTime: Double,
                            indexSize:Int,
                            reverseSearch:Boolean) extends JsonWritable[TotalResultStats] {


  def toCSV = s"$version," +
    s"$seed," +
    s"$queryFileName," +
    s"$sampleSize," +
    s"$bloomFilterSize," +
    s"$timeSliceChoiceMethod," +
    s"$numTimeSliceIndices," +
    s"$dataLoadingTimeMS," +
    s"$requirecValuesIndexBuildTime," +
    s"$summedTimeSliceIndicesBuildTimes," +
    s"$totalIndexQueryTime," +
    s"$totalSubsetValidationTime," +
    s"$totalTemporalValidationTime," +
    s"${indexParameters.absoluteEpsilon}," +
    s"${indexParameters.absDeltaInNanos}," +
    s"${indexParameters.omega}," +
    s"${queryParameters.absoluteEpsilon}," +
    s"${queryParameters.absDeltaInNanos}," +
    s"${queryParameters.omega}," +
    s"${indexSize}," +
    s"$reverseSearch"
}

object TotalResultStats {

  def schema = "version," +
    "seed," +
    "queryFileName," +
    "sampleSize," +
    "bloomFilterSize," +
    "timeSliceChoiceMethod," +
    "numTimeSliceIndices," +
    "dataLoadingTimeMS," +
    "requirecValuesIndexBuildTime," +
    "summedTimeSliceIndicesBuildTimes," +
    "totalIndexQueryTime," +
    "totalSubsetValidationTime," +
    "totalTemporalValidationTime," +
    "indexEpsilon," +
    "indexDelta," +
    "indexOmega," +
    "queryEpsilon," +
    "queryDelta," +
    "queryOmega," +
    "indexSize," +
    "reverseSearch"
}
