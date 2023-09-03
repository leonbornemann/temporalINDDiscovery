package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.ColumnHistoryID
import de.hpi.temporal_ind.data.attribute_history.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.data.ind.{EpsilonOmegaDeltaRelaxedTemporalIND, ValidationVariant}
import de.hpi.temporal_ind.discovery.input_data.{ColumnHistoryStorage, EnrichedColumnHistory, InputDataManager}

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await

abstract class TINDDiscoveryAlgorithm(dataManager: InputDataManager,
                                      reverseSearch:Boolean,
                                      var nThreads:Int) {

  var historiesEnriched: ColumnHistoryStorage = null
  var historiesEnrichedOriginal: ColumnHistoryStorage = null
  var dataLoadingTimeMS: Double = 0.0
  var allPairsMode = false

  def getQueries(queryFile: File) = {
    val queryIDs = ColumnHistoryID
      .fromJsonObjectPerLineFile(queryFile.getAbsolutePath)
      .toSet
    historiesEnrichedOriginal
      .histories
      .filter(h => queryIDs.contains(h.och.columnHistoryID))
      .zipWithIndex
  }

  def initData() = {
    val (historiesEnriched: ColumnHistoryStorage, dataLoadingTimeMS: Double) = loadData()
    this.historiesEnrichedOriginal = historiesEnriched
    this.historiesEnriched = historiesEnriched
    this.dataLoadingTimeMS = dataLoadingTimeMS
  }

  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och)))
  }

  def useSubsetOfData(inputSizeFactor: Int) = {
    assert(inputSizeFactor <= historiesEnrichedOriginal.histories.size)
    this.historiesEnriched = new ColumnHistoryStorage(historiesEnrichedOriginal.histories.take(inputSizeFactor))
    // data loading time is now longer correct now, but that does not matter for the query use-case
  }

  def loadData() = {
    val beforePreparation = System.nanoTime()
    val histories = dataManager.loadData()
    val historiesEnriched = enrichWithHistory(histories)
    //required values index:
    val afterPreparation = System.nanoTime()
    val dataLoadingTimeMS = (afterPreparation - beforePreparation) / 1000000.0
    (historiesEnriched, dataLoadingTimeMS)
  }

  def validateCandidates(query: EnrichedColumnHistory,
                         queryParameters: TINDParameters,
                         candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]): collection.IndexedSeq[EpsilonOmegaDeltaRelaxedTemporalIND[String]] = {
    val batchSize = GLOBAL_CONFIG.PARALLEL_TIND_VALIDATION_BATCH_SIZE
    if (candidatesRequiredValues.size < batchSize || nThreads == 1 || allPairsMode) {
      val res = candidatesRequiredValues.map(refCandidate => {
        //new TemporalShifted
        val lhs = if (!reverseSearch) query.och else refCandidate.och
        val rhs = if (!reverseSearch) refCandidate.och else query.och
        new EpsilonOmegaDeltaRelaxedTemporalIND[String](lhs, rhs, queryParameters, ValidationVariant.FULL_TIME_PERIOD)
      }).filter(_.isValid)
      res
    } else {
      //do it in parallel
      val batches = candidatesRequiredValues.grouped(5).toIndexedSeq
      val handler = new ParallelExecutionHandler(batches.size)
      val futures = batches.map { batch =>
        val f = handler.addAsFuture(batch.map(refCandidate => {
          val lhs = if (!reverseSearch) query.och else refCandidate.och
          val rhs = if (!reverseSearch) refCandidate.och else query.och
          new EpsilonOmegaDeltaRelaxedTemporalIND[String](lhs, rhs, queryParameters, ValidationVariant.FULL_TIME_PERIOD)
        }).filter(_.isValid))
        f
      }
      handler.awaitTermination()
      val results = futures.flatMap(f => {
        val res = Await.result(f, scala.concurrent.duration.Duration.MinusInf)
        res
      })
      results
    }
  }


}
