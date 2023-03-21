package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.metanome.algorithms.many.bitvectors.BitVector

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

class MultiLevelIndexStructure(val indexEntireValueset: BloomfilterIndex,
                               val timeSliceIndices: collection.SortedMap[(Instant, Instant), BloomfilterIndex],
                               val requiredValuesIndexBuildTime: Double,
                               val timeSliceIndexBuildTimes: ArrayBuffer[Double]) {
  def bitVectorToColumns(curCandidates: BitVector[_]) = indexEntireValueset.bitVectorToColumns(curCandidates)

  /***
   * Validation happens in-place!
   */
  def validateContainments(query: EnrichedColumnHistory, candidates: BitVector[_]) = {
    val (_, subsetValidationTime) = TimeUtil.executionTimeInMS({
      indexEntireValueset.validateContainment(query, candidates)
      timeSliceIndices.foreach(index => index._2.validateContainment(query, candidates))
    })
    subsetValidationTime
  }

  def totalTimeSliceIndexBuildTime: Double = timeSliceIndexBuildTimes.sum

  def limitTimeSliceIndices(numTimeSliceIndices: Int) = new MultiLevelIndexStructure(indexEntireValueset,
    timeSliceIndices.take(numTimeSliceIndices),
    requiredValuesIndexBuildTime,
    timeSliceIndexBuildTimes.take(numTimeSliceIndices))

  def queryRequiredValuesIndex(query: EnrichedColumnHistory) = {
    val (candidatesRequiredValues, queryTime, _) = indexEntireValueset.queryWithBitVectorResult(query,
      None,
      false)
    (candidatesRequiredValues, queryTime)
  }

  def queryTimeSliceIndices(query: EnrichedColumnHistory,initialCandidates: BitVector[_]) = {
    var curCandidates = initialCandidates
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    val queryAndValidationTimes = timeSliceIndices
      .zipWithIndex
      .map { case (((begin, end), index), indexOrder) => {
        val (candidatesIndexSlice, queryTimeSliceTime, timeSliceValidationTime) = index.queryWithBitVectorResult(query,
          Some(curCandidates), false)
        curCandidates = candidatesIndexSlice
        queryTimesSlices += queryTimeSliceTime
        (queryTimeSliceTime, timeSliceValidationTime)
      }
      }
    val queryTimeTotal = queryAndValidationTimes.map(_._1).sum
    val validationTimeTotal = queryAndValidationTimes.map(_._2).sum
    (curCandidates, queryTimeTotal)
  }

}
