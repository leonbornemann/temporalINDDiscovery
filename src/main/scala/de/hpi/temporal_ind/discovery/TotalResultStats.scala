package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.JsonWritable

case class TotalResultStats(numTimeSliceIndices: Int, dataLoadingTimeMS: Double, requirecValuesIndexBuildTime: Double, timeSliceIndexBuildTimes: Double, totalQueryTime: Double, totalSubsetValidationTime: Double, totalTemporalValidationTime: Double) extends JsonWritable[TotalResultStats]{


}
