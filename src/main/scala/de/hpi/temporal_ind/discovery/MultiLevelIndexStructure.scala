package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.metanome.algorithms.many.bitvectors.BitVector

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

class MultiLevelIndexStructure(val indexEntireValueset: BloomfilterIndex,
                               val timeSliceIndices: MultiTimeSliceIndexStructure,
                               val requiredValuesIndexBuildTime: Double) {
  def bitVectorToColumns(curCandidates: BitVector[_]) = indexEntireValueset.bitVectorToColumns(curCandidates)

  /***
   * Validation happens in-place!
   */
  def validateContainments(query: EnrichedColumnHistory, candidates: BitVector[_]) = {
    val (_, subsetValidationTime) = TimeUtil.executionTimeInMS({
      indexEntireValueset.validateContainment(query, candidates)
      timeSliceIndices.validateContainments(query, candidates)
    })
    subsetValidationTime
  }

  def totalTimeSliceIndexBuildTime: Double = timeSliceIndices.timeSliceIndexBuildTimes.sum

  def limitTimeSliceIndices(numTimeSliceIndices: Int) =
    new MultiLevelIndexStructure(indexEntireValueset,
    timeSliceIndices.limitTimeSliceIndices(numTimeSliceIndices),
    requiredValuesIndexBuildTime)

  def queryRequiredValuesIndex(query: EnrichedColumnHistory) = {
    val (candidatesRequiredValues, queryTime, _) = indexEntireValueset.queryWithBitVectorResult(query,
      None,
      false)
    (candidatesRequiredValues, queryTime)
  }

  def queryTimeSliceIndices(query: EnrichedColumnHistory,initialCandidates: BitVector[_]) = {
    timeSliceIndices.executeQuery(query,initialCandidates)

  }

}
