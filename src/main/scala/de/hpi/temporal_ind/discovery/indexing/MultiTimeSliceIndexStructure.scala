package de.hpi.temporal_ind.discovery.indexing

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.{EnrichedColumnHistory, ValuesInTimeWindow}
import de.metanome.algorithms.many.bitvectors.BitVector

import java.time.Instant

class MultiTimeSliceIndexStructure(val timeSliceIndices: collection.SortedMap[(Instant, Instant), BloomfilterIndex],
                                   val timeSliceIndexBuildTimes: collection.IndexedSeq[Double]
                                  ) {

  def executeQuery(query: EnrichedColumnHistory, queryParameters: TINDParameters, initialCandidates: BitVector[_],reverseSearch:Boolean) = {
    val candidateToViolationMap = collection.mutable.HashMap[Int,Double]()
    var curCandidates = initialCandidates
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    val queryAndValidationTimes = timeSliceIndices
      .map { case ((begin, end), index) => {
        val (candidatesIndexSlice, queryTimeSliceTime, _) = if(!reverseSearch){
          val queryValueSets = query.getValueSetsInWindow( begin, end)
          index.queryWithBitVectorResult(queryValueSets,
            queryParameters,
            Some(curCandidates),
            Some(candidateToViolationMap))
        } else {
          val beginQuery = begin.minusNanos( 2*queryParameters.absDeltaInNanos)
          val endQuery = end.plusNanos( 2*queryParameters.absDeltaInNanos)
          val beginIndex = begin.minusNanos( queryParameters.absDeltaInNanos)
          val endIndex = end.plusNanos( queryParameters.absDeltaInNanos)
          val values = query.valueSetInWindow(beginQuery, endQuery)
          val queryValueSet = new ValuesInTimeWindow(beginIndex,endIndex,values)
          index.queryWithBitVectorResultReverseSearch(queryValueSet,
            queryParameters,
            Some(curCandidates),
            Some(candidateToViolationMap))
        }
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
      val queryValueSets = query.getValueSetsInWindow(begin,end)
      index.validateContainmentOfSets(queryValueSets,queryParameters, candidates, actualViolationMap)
    }
  }

  def validateContainmentsReverseQuery(query: EnrichedColumnHistory, queryParameters: TINDParameters, candidates: BitVector[_]) = {
    val actualViolationMap = Some(collection.mutable.HashMap[Int, Double]())
    timeSliceIndices.foreach { case ((begin, end), index) =>
      val beginQuery = begin.minusNanos(2 * queryParameters.absDeltaInNanos)
      val endQuery = end.plusNanos(2 * queryParameters.absDeltaInNanos)
      val values = query.valueSetInWindow(beginQuery, endQuery)
      val beginIndex = begin.minusNanos(queryParameters.absDeltaInNanos)
      val endIndex = end.plusNanos(queryParameters.absDeltaInNanos)
      val queryValueSet = new ValuesInTimeWindow(beginIndex,endIndex,values)
      index.validateContainmentOfSetsReverseSearchTimeSliceIndex(queryValueSet, queryParameters, candidates, actualViolationMap)
    }
  }

}
