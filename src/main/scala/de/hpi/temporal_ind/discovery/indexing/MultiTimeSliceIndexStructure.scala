package de.hpi.temporal_ind.discovery.indexing

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.input_data.{EnrichedColumnHistory, ValuesInTimeWindow}
import de.metanome.algorithms.many.bitvectors.BitVector

import java.time.Instant

class MultiTimeSliceIndexStructure(val timeSliceIndices: collection.SortedMap[(Instant, Instant), BloomfilterIndex],
                                   val timeSliceIndexBuildTimes: collection.IndexedSeq[Double]
                                  ) {


  def executeQuery(query: EnrichedColumnHistory, queryParameters: TINDParameters, initialCandidates: BitVector[_]) = {
    val candidateToViolationMap = collection.mutable.HashMap[Int,Double]()
    var curCandidates = initialCandidates
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    val queryAndValidationTimes = timeSliceIndices
      .map { case ((begin, end), index) => {
        val queryValueSets =  query.och.versionsInWindowNew(begin,end).map(t => new ValuesInTimeWindow(begin,end,t._2.values)).toSeq
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
      val queryValueSets =  query.och.versionsInWindowNew(begin,end).map(t => new ValuesInTimeWindow(begin,end,t._2.values)).toSeq
      index.validateContainmentOfSets(queryValueSets,queryParameters, candidates, actualViolationMap)
    }

  }

}
