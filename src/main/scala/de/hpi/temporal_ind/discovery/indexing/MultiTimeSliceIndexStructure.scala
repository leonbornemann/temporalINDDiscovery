package de.hpi.temporal_ind.discovery.indexing

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.{EnrichedColumnHistory, ValuesInTimeWindow}
import de.metanome.algorithms.many.bitvectors.BitVector

import java.time.Instant

class MultiTimeSliceIndexStructure(val timeSliceIndices: collection.SortedMap[(Instant, Instant), BloomfilterIndex],
                                   val timeSliceIndexBuildTimes: collection.IndexedSeq[Double]
                                  ) {


  def getValueSetsInWindow(query: EnrichedColumnHistory, begin: Instant, end: Instant) = {
    val withIndex = query.och.versionsInWindowNew(begin, end)
      .toIndexedSeq
      .zipWithIndex
    val queryValueSets = withIndex
      .map { case ((begin, version), i) =>
        val curEnd = if (i == withIndex.size - 1) end else withIndex(i + 1)._1._1
        new ValuesInTimeWindow(begin, curEnd, version.values)
      }
    queryValueSets
  }

  def executeQuery(query: EnrichedColumnHistory, queryParameters: TINDParameters, initialCandidates: BitVector[_]) = {
    val candidateToViolationMap = collection.mutable.HashMap[Int,Double]()
    var curCandidates = initialCandidates
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    val queryAndValidationTimes = timeSliceIndices
      .map { case ((begin, end), index) => {
        val queryValueSets = getValueSetsInWindow(query,begin,end)
        val (candidatesIndexSlice, queryTimeSliceTime, _) = index.queryWithBitVectorResult(queryValueSets,
          queryParameters,
          Some(curCandidates),
          Some(candidateToViolationMap))
        curCandidates = candidatesIndexSlice
        queryTimesSlices += queryTimeSliceTime
        queryTimeSliceTime
      }
      }
    val queryTimeTotal = queryAndValidationTimes.sum
    (curCandidates, queryTimeTotal)
  }

  def limitTimeSliceIndices(numTimeSliceIndices: Int): MultiTimeSliceIndexStructure =
    new MultiTimeSliceIndexStructure(timeSliceIndices.take(numTimeSliceIndices),timeSliceIndexBuildTimes.take(numTimeSliceIndices))

  def validateContainments(query: EnrichedColumnHistory, queryParameters: TINDParameters, candidates: BitVector[_]) = {
    val actualViolationMap = Some(collection.mutable.HashMap[Int, Double]())
    timeSliceIndices.foreach{case ((begin,end),index) =>
      val queryValueSets = getValueSetsInWindow(query,begin,end)
      index.validateContainmentOfSets(queryValueSets,queryParameters, candidates, actualViolationMap)
    }

  }

}
