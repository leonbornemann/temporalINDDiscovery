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
                                 version:String) {

  def toCSVLine = s"$queryNumber,$numTimeSliceIndices,$totalIndexQueryTime,$totalSubsetValidationTime,$temporalValidationTime,$numCandidatesAfterIndexQuery,$numCandidatesAfterSubsetValidation,$truePositives,$avgVersionsPerTimeSliceWindow,$version"
}

object IndividualResultStats {
  def schema = "queryNumber,numTimeSliceIndices,totalIndexQueryTime,totalSubsetValidationTime,temporalValidationTime,numCandidatesAfterIndexQuery,numCandidatesAfterSubsetValidation,truePositives,avgVersionsPerTimeSliceWindow,version"
}
