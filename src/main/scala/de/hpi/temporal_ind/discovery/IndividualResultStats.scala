package de.hpi.temporal_ind.discovery

case class IndividualResultStats(queryNumber: Int, numTimeSliceIndices: Int, totalIndexQueryTime: Double, totalSubsetValidationTime: Double, temporalValidationTime: Double,inputSize:Int,truePositives:Int) {

  def toCSVLine = s"$queryNumber,$numTimeSliceIndices,$totalIndexQueryTime,$totalSubsetValidationTime,$temporalValidationTime,$inputSize,$truePositives"
}

object IndividualResultStats {
  def schema = "queryNumber,numTimeSliceIndices,totalIndexQueryTime,totalSubsetValidationTime,temporalValidationTime,inputSize,truePositives"
}
