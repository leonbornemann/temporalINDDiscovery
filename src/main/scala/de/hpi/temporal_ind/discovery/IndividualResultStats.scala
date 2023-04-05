package de.hpi.temporal_ind.discovery

case class IndividualResultStats(queryNumber: Int,
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
                                 violationTrackingEnabled: Boolean,
                                 timeSliceChoiceMethod:TimeSliceChoiceMethod.Value) {

  def toCSVLine = s"$queryNumber," +
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
    s"$violationTrackingEnabled," +
    s"$timeSliceChoiceMethod"
}

object IndividualResultStats {
  def schema = "queryNumber," +
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
    "violationTrackingEnabled," +
    "timeSliceChoiceMethod"
}
