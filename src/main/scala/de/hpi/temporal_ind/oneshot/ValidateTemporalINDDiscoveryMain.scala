package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.file_search.IndexedColumnHistories
import de.hpi.temporal_ind.data.ind.weight_functions.ConstantWeightFunction
import de.hpi.temporal_ind.data.ind.{INDCandidateIDs, EpsilonOmegaDeltaRelaxedTemporalIND, ValidationVariant}
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.util.TimeUtil

import java.time.temporal.ChronoUnit

object ValidateTemporalINDDiscoveryMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val pathToINDs = "/home/leon/data/temporalINDDiscovery/wikipedia/discovery/testOutput/0.4/discoveredINDs.jsonl"
  val columnHistoryPath = "/home/leon/data/temporalINDDiscovery/wikipedia/columnHistoriesTestSample/"
  val candidates = INDCandidateIDs.fromJsonObjectPerLineFile(pathToINDs)
  val index = IndexedColumnHistories.fromColumnHistoryJsonPerLineDir(columnHistoryPath)
  val deltaInNanos = TimeUtil.nanosPerDay*30
  val epsilon = 0.050
  val absoluteEpsilonInNanos = (GLOBAL_CONFIG.totalTimeInNanos*epsilon).toLong
  val absoluteEpsilonInDays = (GLOBAL_CONFIG.totalTimeInDays*epsilon).toLong
  val allDays = (0L until GLOBAL_CONFIG.totalTimeInDays).map(l => {
    val instant = GLOBAL_CONFIG.earliestInstant.plus(l, ChronoUnit.DAYS)
    val leftDelta = instant.minusNanos(deltaInNanos)
    val rightDelta = instant.plusNanos(deltaInNanos)
    (instant,leftDelta,rightDelta)
  })
  //correctness:
  candidates.zipWithIndex.foreach{case (c,i) => {
    if(i%10000==0)
      println("Finished ",i," out of ",candidates.size)
    val lhs = index.multiLevelIndex(c.lhsPageID)(c.lhsColumnID).asOrderedHistory
    val rhs = index.multiLevelIndex(c.rhsPageID)(c.rhsColumnID).asOrderedHistory
    val ind = new EpsilonOmegaDeltaRelaxedTemporalIND[String](lhs,rhs,TINDParameters(absoluteEpsilonInNanos,deltaInNanos,new ConstantWeightFunction()),ValidationVariant.FULL_TIME_PERIOD)
    //use a very simple variant to validate:
    val violationSum = allDays.map{ case (d,l,r) => {
      val left = lhs.versionAt(d).values
      val right = rhs.valueSetInWindow(l,r)
      if(left.subsetOf(right)) 0 else 1
    }}.sum
    assert(violationSum<absoluteEpsilonInDays)
    //TODO
    assert(ind.isValid)
  }}
}
