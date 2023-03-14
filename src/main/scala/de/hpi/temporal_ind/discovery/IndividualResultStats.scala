package de.hpi.temporal_ind.discovery

case class IndividualResultStats(queryNumber: Int, numTimeSliceIndices: Int, totalIndexQueryTime: Double, totalSubsetValidationTime: Double, temporalValidationTime: Double) {

  def toCSVLine = s"$queryNumber,$numTimeSliceIndices,$totalIndexQueryTime,$totalSubsetValidationTime,$temporalValidationTime"
}

object IndividualResultStats {
  def schema = "queryNumber,numTimeSliceIndices,totalIndexQueryTime,totalSubsetValidationTime,temporalValidationTime"
}
