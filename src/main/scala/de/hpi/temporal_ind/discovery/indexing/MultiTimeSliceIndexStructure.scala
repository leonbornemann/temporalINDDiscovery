package de.hpi.temporal_ind.discovery.indexing

import de.hpi.temporal_ind.discovery.input_data.EnrichedColumnHistory
import de.metanome.algorithms.many.bitvectors.BitVector

import java.time.Instant

class MultiTimeSliceIndexStructure(val timeSliceIndices: collection.SortedMap[(Instant, Instant), BloomfilterIndex],
                                   val timeSliceIndexBuildTimes: collection.IndexedSeq[Double],
                                   val enableEarlyAbort:Boolean,
                                   val absoluteEpsilonNanos:Long
                                  ) {


  def executeQuery(query: EnrichedColumnHistory, initialCandidates: BitVector[_]) = {
    val candidateToViolationMap = if(enableEarlyAbort) Some(collection.mutable.HashMap[Int,Long]()) else None
    var curCandidates = initialCandidates
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    val queryAndValidationTimes = timeSliceIndices
      .map { case ((begin, end), index) => {
        val (candidatesIndexSlice, queryTimeSliceTime, _) = index.queryWithBitVectorResult(query,
          Some(curCandidates),
          false,
          candidateToViolationMap)
        curCandidates = candidatesIndexSlice
        queryTimesSlices += queryTimeSliceTime
        queryTimeSliceTime
      }
      }
    val queryTimeTotal = queryAndValidationTimes.sum
    (curCandidates, queryTimeTotal)
  }

  def limitTimeSliceIndices(numTimeSliceIndices: Int): MultiTimeSliceIndexStructure =
    new MultiTimeSliceIndexStructure(timeSliceIndices.take(numTimeSliceIndices),timeSliceIndexBuildTimes.take(numTimeSliceIndices),enableEarlyAbort, absoluteEpsilonNanos)

  def validateContainments(query: EnrichedColumnHistory, candidates: BitVector[_]) =
    timeSliceIndices.foreach(index => index._2.validateContainment(query, candidates))

}