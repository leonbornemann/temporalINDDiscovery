package de.hpi.temporal_ind.discovery.indexing

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.EnrichedColumnHistory
import de.metanome.algorithms.many.bitvectors.BitVector
import de.hpi.temporal_ind.discovery.input_data.ValuesInTimeWindow

class MultiLevelIndexStructure(val indexEntireValueset: BloomfilterIndex,
                               val timeSliceIndices: MultiTimeSliceIndexStructure,
                               val requiredValuesIndexBuildTime: Double,
                              ) {
  def bitVectorToColumns(curCandidates: BitVector[_]) = indexEntireValueset.bitVectorToColumns(curCandidates)

  /***
   * Validation happens in-place!
   */
  def validateContainments(query: EnrichedColumnHistory, queryParameters: TINDParameters, candidates: BitVector[_]) = {
    val (_, subsetValidationTime) = TimeUtil.executionTimeInMS({
      val requiredValues = new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant,query.requiredValues(queryParameters))
      indexEntireValueset.validateContainmentOfSets(IndexedSeq(requiredValues),queryParameters, candidates)
      timeSliceIndices.validateContainments(query,queryParameters, candidates)
    })
    subsetValidationTime
  }

  def totalTimeSliceIndexBuildTime: Double = timeSliceIndices.timeSliceIndexBuildTimes.sum

  def limitTimeSliceIndices(numTimeSliceIndices: Int) =
    new MultiLevelIndexStructure(indexEntireValueset,
    timeSliceIndices.limitTimeSliceIndices(numTimeSliceIndices),
    requiredValuesIndexBuildTime)

  def queryRequiredValuesIndex(query: EnrichedColumnHistory,queryParameters:TINDParameters) = {
    val requiredValues = new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant,query.requiredValues(queryParameters))
    val (candidatesRequiredValues, queryTime, _) = indexEntireValueset.queryWithBitVectorResult(IndexedSeq(requiredValues),
      queryParameters,
      None)
    (candidatesRequiredValues, queryTime)
  }

  def queryTimeSliceIndices(query: EnrichedColumnHistory,queryParamters:TINDParameters,initialCandidates: BitVector[_]) = {
    timeSliceIndices.executeQuery(query,queryParamters,initialCandidates)

  }

}
