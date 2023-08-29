package de.hpi.temporal_ind.discovery.indexing

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.EnrichedColumnHistory
import de.metanome.algorithms.many.bitvectors.BitVector
import de.hpi.temporal_ind.discovery.input_data.ValuesInTimeWindow
import de.hpi.temporal_ind.util.TimeUtil

class MultiLevelIndexStructure(val indexEntireValueset: Option[BloomfilterIndex],
                               val timeSliceIndices: MultiTimeSliceIndexStructure,
                               val requiredValuesIndexBuildTime: Double,
                              ) {
  def bitVectorToColumns(curCandidates: BitVector[_]) = {
    if(indexEntireValueset.isDefined)
      indexEntireValueset.get.bitVectorToColumns(curCandidates)
    else
      timeSliceIndices.timeSliceIndices.head._2.bitVectorToColumns(curCandidates)
  }

  /***
   * Validation happens in-place!
   */
  def validateContainments(query: EnrichedColumnHistory, queryParameters: TINDParameters, candidates: BitVector[_],reverseSearch:Boolean) = {
    val (_, subsetValidationTime) = TimeUtil.executionTimeInMS({
      if(!reverseSearch){
        val requiredValues = new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant, GLOBAL_CONFIG.lastInstant, query.requiredValues(queryParameters))
        if(indexEntireValueset.isDefined)
          indexEntireValueset.get.validateContainmentOfSets(IndexedSeq(requiredValues), queryParameters, candidates)
        timeSliceIndices.validateContainments(query, queryParameters, candidates)
      } else {
        val allValues = new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant, GLOBAL_CONFIG.lastInstant, query.allValues)
        if(indexEntireValueset.isDefined)
          indexEntireValueset.get.validateContainmentOfSetsReverseQueryRequiredValues(allValues, queryParameters, candidates)
        timeSliceIndices.validateContainmentsReverseQuery(query, queryParameters, candidates)
      }
    })
    subsetValidationTime
  }

  def totalTimeSliceIndexBuildTime: Double = timeSliceIndices.timeSliceIndexBuildTimes.sum

  def limitTimeSliceIndices(numTimeSliceIndices: Int) =
    new MultiLevelIndexStructure(indexEntireValueset,
    timeSliceIndices.limitTimeSliceIndices(numTimeSliceIndices),
    requiredValuesIndexBuildTime)

  def queryRequiredValuesIndex(query: EnrichedColumnHistory,queryParameters:TINDParameters,reverseSearch:Boolean) = {
    assert(indexEntireValueset.isDefined)
    if(!reverseSearch){
      val queryValueSet = new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant, GLOBAL_CONFIG.lastInstant, query.requiredValues(queryParameters))
      val (candidatesRequiredValues, queryTime, _) = indexEntireValueset.get.queryWithBitVectorResult(IndexedSeq(queryValueSet),
        queryParameters,
        None)
      (candidatesRequiredValues, queryTime)
    } else {
      val queryValueSet = new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant, GLOBAL_CONFIG.lastInstant, query.allValues)
      val (candidatesRequiredValues, queryTime, _) = indexEntireValueset.get.queryWithBitVectorResultReverseSearchRequiredValues(IndexedSeq(queryValueSet),
        queryParameters)
      (candidatesRequiredValues, queryTime)
    }
  }

  def queryTimeSliceIndices(query: EnrichedColumnHistory,queryParamters:TINDParameters,initialCandidates: BitVector[_],reverseSearch:Boolean) = {
    timeSliceIndices.executeQuery(query,queryParamters,initialCandidates,reverseSearch)

  }

}
