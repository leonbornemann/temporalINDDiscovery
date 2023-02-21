package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.original.{OrderedColumnHistory, ValidationVariant}
import de.hpi.temporal_ind.data.ind.SimpleTimeWindowTemporalIND
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

class RelaxedShiftedTemporalINDDiscovery(sourceDir: File, targetDir: File, epsilon: Double, deltaInNanos: Long) extends StrictLogging{

  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos*epsilon).toLong
  val historiesEnriched = TimeUtil.printExecutionTimeInMS(enrichWithHistory(loadHistories()),"Column History Metadata Enrichtment")


  def loadHistories() = OrderedColumnHistory
    .readFromFiles(sourceDir)
    .toIndexedSeq

  def getRequiredValues(indexedSeq: IndexedSeq[OrderedColumnHistory]) = indexedSeq
    .map(och => (och,och.history.requiredValues(absoluteEpsilonNanos)))
    .toMap


  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och,absoluteEpsilonNanos)))
  }

  def getIndexForEntireValueset() = {
    new BloomfilterIndex(historiesEnriched.histories,((e:EnrichedColumnHistory) => e.allValues))
  }

  def validateCandidates(query:EnrichedColumnHistory,candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]) = {
    candidatesRequiredValues.map(refCandidate => {
      //new TemporalShifted
      new SimpleTimeWindowTemporalIND[String](query.och, refCandidate.och, deltaInNanos, absoluteEpsilonNanos, false, ValidationVariant.FULL_TIME_PERIOD)
    }).filter(_.isValid)
  }

  def runDiscovery() = {
    val resultPR = new PrintWriter(targetDir + "/discoveredINDs.jsonl")
    val statsPR = new PrintWriter(targetDir + "/discoveryStats.jsonl")
    val timeStatsPR = new PrintWriter(targetDir + "/discoveryRuntimes.csv")
    statsPR.println("truePositiveCount,falsePositiveCount")
    timeStatsPR.println("Type,Time [MS]")
    val (bloomFilterIndexEntireValueset,timeIndexBuild) = TimeUtil.executionTimeInMS(getIndexForEntireValueset())
    //query all:
    timeStatsPR.println(s"Index Build,$timeIndexBuild")
    historiesEnriched.histories.foreach(query => {
      //TODO: make query return the bitVector once we do more filtering
      val (candidatesRequiredValues,queryTime) = TimeUtil.executionTimeInMS(bloomFilterIndexEntireValueset.query(query,((e:EnrichedColumnHistory) => e.requiredValues)))
      val (trueTemporalINDs,validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query,candidatesRequiredValues))
      val truePositiveCount = trueTemporalINDs.size
      val falsePositiveCount = candidatesRequiredValues.size-truePositiveCount
      trueTemporalINDs.foreach(c => c.toCandidateIDs.appendToWriter(resultPR))
      statsPR.println(s"$truePositiveCount,$falsePositiveCount")
      timeStatsPR.println(s"Query,$queryTime")
      timeStatsPR.println(s"Validation,$validationTime")
    })
    resultPR.close()
    statsPR.close()
    timeStatsPR.close()
  }

  def discover() = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery()
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

}
