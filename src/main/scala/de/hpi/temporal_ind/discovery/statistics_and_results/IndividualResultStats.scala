package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod

case class IndividualResultStats(queryNumber: Int,
                                 queryFileName:String,
                                 indexParameters: TINDParameters,
                                 queryParameters: TINDParameters,
                                 seed:Long,
                                 numTimeSliceIndices: Int,
                                 totalIndexQueryTime: Double,
                                 totalSubsetValidationTime: Double,
                                 temporalValidationTime: Double,
                                 numCandidatesAfterIndexQuery:Int,
                                 numCandidatesAfterSubsetValidation:Int,
                                 truePositives:Int,
                                 avgVersionsPerTimeSliceWindow:Double,
                                 version:String,
                                 sampleSize: Int,
                                 bloomFilterSize: Int,
                                 timeSliceChoiceMethod:TimeSliceChoiceMethod.Value) {

  def toCSVLine = s"$queryNumber," +
    s"$queryFileName," +
    s"$seed," +
    s"$numTimeSliceIndices," +
    s"$totalIndexQueryTime," +
    s"$totalSubsetValidationTime," +
    s"$temporalValidationTime," +
    s"$numCandidatesAfterIndexQuery," +
    s"$numCandidatesAfterSubsetValidation," +
    s"$truePositives," +
    s"$avgVersionsPerTimeSliceWindow," +
    s"$version," +
    s"$sampleSize," +
    s"$bloomFilterSize," +
    s"$timeSliceChoiceMethod," +
    s"${indexParameters.absoluteEpsilon}," +
    s"${indexParameters.absDeltaInNanos}," +
    s"${indexParameters.omega}," +
    s"${queryParameters.absoluteEpsilon}," +
    s"${queryParameters.absDeltaInNanos}," +
    s"${queryParameters.omega}"
}

object IndividualResultStats {
  def schema = "queryNumber," +
    "queryFileName," +
    "seed," +
    "numTimeSliceIndices," +
    "totalIndexQueryTime," +
    "totalSubsetValidationTime," +
    "temporalValidationTime," +
    "numCandidatesAfterIndexQuery," +
    "numCandidatesAfterSubsetValidation," +
    "truePositives," +
    "avgVersionsPerTimeSliceWindow," +
    "version," +
    "sampleSize," +
    "bloomFilterSize," +
    "timeSliceChoiceMethod," +
    "indexEpsilon," +
    "indexDelta," +
    "indexOmega," +
    "queryEpsilon," +
    "queryDelta," +
    "queryOmega"
}
