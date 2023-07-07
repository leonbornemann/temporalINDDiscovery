package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.ColumnHistoryID
import de.hpi.temporal_ind.data.attribute_history.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.data.ind.{EpsilonOmegaDeltaRelaxedTemporalIND, ValidationVariant}
import de.hpi.temporal_ind.discovery.indexing.time_slice_choice.{DynamicWeightedRandomTimeSliceChooser, TimeSliceChooser, WeightedShuffledTimestamps}
import de.hpi.temporal_ind.discovery.indexing.{BloomfilterIndex, MultiLevelIndexStructure, MultiTimeSliceIndexStructure, TimeSliceChoiceMethod}
import de.hpi.temporal_ind.discovery.input_data.{ColumnHistoryStorage, EnrichedColumnHistory, InputDataManager}
import de.hpi.temporal_ind.discovery.statistics_and_results._
import de.hpi.temporal_ind.util.TimeUtil

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
//import org.nustaq.serialization.{FSTConfiguration, FSTObjectInput, FSTObjectOutput}
//import org.nustaq.serialization.util.FSTOutputStream

import java.io.File
import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class TINDSearcher(val dataManager:InputDataManager,
                   val subsetValidation:Boolean,
                   val timeSliceChoiceMethod:TimeSliceChoiceMethod.Value,
                   var nThreads:Int,
                   val metaDir:File,
                   val reverseSearch:Boolean) extends StrictLogging{

  val version = "0.99"
  var allPairsMode = false
  var fullMultiIndexStructure:MultiLevelIndexStructure = null
  var historiesEnriched:ColumnHistoryStorage = null
  var historiesEnrichedOriginal:ColumnHistoryStorage = null
  var dataLoadingTimeMS:Double=0.0
  var expectedQueryParameters:TINDParameters = null
  var curResultSerializer: StandardResultSerializer = null
  var seed: Long = 0
  var bloomfilterSize: Int = -1
  var random: Random = null

  def useSubsetOfData(inputSizeFactor: Int) = {
    assert(inputSizeFactor <= historiesEnriched.histories.size)
    this.historiesEnriched = new ColumnHistoryStorage(historiesEnrichedOriginal.histories.take(inputSizeFactor))
    // data loading time is now longer correct now, but that does not matter for the query use-case
  }

  def initData() = {
    val (historiesEnriched: ColumnHistoryStorage, dataLoadingTimeMS: Double) = loadData()
    this.historiesEnrichedOriginal = historiesEnriched
    this.historiesEnriched = historiesEnriched
    this.dataLoadingTimeMS = dataLoadingTimeMS
  }

  def buildIndicesWithSeed(maxNumTimeSlicIndices:Int,seed:Long,bloomFilterSize:Int,expectedQueryParameters:TINDParameters) = {
    this.expectedQueryParameters = expectedQueryParameters
    this.seed = seed
    this.random = new Random(seed)
    this.bloomfilterSize=bloomFilterSize
    fullMultiIndexStructure = null
    System.gc()
    val indexBuilder = new TINDIndexBuilder(metaDir, seed, timeSliceChoiceMethod, expectedQueryParameters, random, reverseSearch, bloomfilterSize)
    fullMultiIndexStructure = indexBuilder.buildMultiIndexStructure(historiesEnriched, maxNumTimeSlicIndices)
  }

  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och)))
  }

  def validateCandidates(query:EnrichedColumnHistory,
                         queryParameters:TINDParameters,
                         candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]):collection.IndexedSeq[EpsilonOmegaDeltaRelaxedTemporalIND[String]] = {
    val batchSize = GLOBAL_CONFIG.PARALLEL_TIND_VALIDATION_BATCH_SIZE
    if(candidatesRequiredValues.size<batchSize || nThreads==1 || allPairsMode){
      val res = candidatesRequiredValues.map(refCandidate => {
        //new TemporalShifted
        val lhs = if(!reverseSearch) query.och else refCandidate.och
        val rhs = if(!reverseSearch) refCandidate.och else query.och
        new EpsilonOmegaDeltaRelaxedTemporalIND[String](lhs, rhs, queryParameters, ValidationVariant.FULL_TIME_PERIOD)
      }).filter(_.isValid)
      res
    } else {
      //do it in parallel
      val batches = candidatesRequiredValues.grouped(5).toIndexedSeq
      val handler = new ParallelExecutionHandler(batches.size)
      val futures = batches.map {batch =>
        val f = handler.addAsFuture(batch.map(refCandidate => {
          val lhs = if (!reverseSearch) query.och else refCandidate.och
          val rhs = if (!reverseSearch) refCandidate.och else query.och
          new EpsilonOmegaDeltaRelaxedTemporalIND[String](lhs, rhs, queryParameters, ValidationVariant.FULL_TIME_PERIOD)
        }).filter(_.isValid))
        f
      }
      handler.awaitTermination()
      val results = futures.flatMap(f => {
        val res = Await.result(f,scala.concurrent.duration.Duration.MinusInf)
        res
      })
      results
    }
  }

  def runDiscovery(queryIDsFile:Option[File],
                   numTimeSliceIndicesList:IndexedSeq[Int],
                   queryParameters:TINDParameters,
                   resultSerializer:StandardResultSerializer) = {
    allPairsMode = queryIDsFile.isEmpty
    curResultSerializer = resultSerializer
    val queries = if(queryIDsFile.isDefined){
      val queryIDs = ColumnHistoryID
        .fromJsonObjectPerLineFile(queryIDsFile.get.getAbsolutePath)
        .toSet
      historiesEnrichedOriginal
        .histories
        .filter(h => queryIDs.contains(h.och.columnHistoryID))
        .zipWithIndex
    } else {
      historiesEnriched.histories.zipWithIndex
    }
    logger.debug(s"Processing ${queries.size} queries")
    numTimeSliceIndicesList.foreach(numTimeSliceIndices => {
      logger.debug(s"Processing numTimeSliceIndices=$numTimeSliceIndices")
      val curIndex = fullMultiIndexStructure.limitTimeSliceIndices(numTimeSliceIndices)
      val (totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime) = queryAll(queries,queryIDsFile.getOrElse(new File("allPairs")).getName, curIndex,queryParameters)
      val totalResultSTatsLine = TotalResultStats(version,expectedQueryParameters,queryParameters, seed,queryIDsFile.getOrElse(new File("allPairs")).getName, queries.size, bloomfilterSize, timeSliceChoiceMethod, numTimeSliceIndices, dataLoadingTimeMS, curIndex.requiredValuesIndexBuildTime, curIndex.totalTimeSliceIndexBuildTime, totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime,historiesEnriched.histories.size,reverseSearch)
      curResultSerializer.addTotalResultStats(totalResultSTatsLine)
    })
    curResultSerializer.closeAll()
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

  def processSingleQuery(curResultSerializer: ResultSerializer,
                         multiLevelIndexStructure: MultiLevelIndexStructure,
                         query: EnrichedColumnHistory,
                         queryParameters: TINDParameters,
                         queryFileName: String,
                         queryNumber: Int,
                         sampleSize:Int) = {
    if(query.och.lifetimeIsBelowThreshold(queryParameters)){
      //just skip this - it is contained in all others!
      logger.debug(s"Skipping query $queryNumber, because it is contained in all other queries")
      (0.0, 0.0, 0.0)
    } else {
      val (candidatesRequiredValues, queryTimeRQValues) = multiLevelIndexStructure.queryRequiredValuesIndex(query, queryParameters,reverseSearch)
      val (curCandidates, queryTimeIndexTimeSlice) = multiLevelIndexStructure.queryTimeSliceIndices(query, queryParameters, candidatesRequiredValues,reverseSearch)
      val numCandidatesAfterIndexQuery = curCandidates.count() - 1
      //validate curCandidates after all candidates have already been pruned
      val subsetValidationTime = if (subsetValidation) {
        multiLevelIndexStructure.validateContainments(query, queryParameters, curCandidates,reverseSearch)
      } else {
        0.0
      }
      val candidateLineages = multiLevelIndexStructure
        .bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
        .filter(_ != query)
      val numCandidatesAfterSubsetValidation = candidateLineages.size
      val (validationTime, truePositiveCount) = validate(query, queryParameters, candidateLineages,curResultSerializer)
      val validationStatRow = BasicQueryInfoRow(queryNumber,
        queryFileName,
        query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant),
        query.och.pageID,
        query.och.tableId,
        query.och.id)
      curResultSerializer.addBasicQueryInfoRow(validationStatRow)
      val avgVersionsPerTimeSliceWindow = multiLevelIndexStructure
        .timeSliceIndices
        .timeSliceIndices
        .keys
        .map { case (s, e) => query.och.versionsInWindow(s, e).size }
        .sum / multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.size.toDouble
      val individualStatLine = IndividualResultStats(queryNumber,
        queryFileName,
        expectedQueryParameters,
        queryParameters,
        seed,
        multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.size,
        queryTimeRQValues + queryTimeIndexTimeSlice,
        subsetValidationTime,
        validationTime,
        numCandidatesAfterIndexQuery,
        numCandidatesAfterSubsetValidation,
        truePositiveCount,
        avgVersionsPerTimeSliceWindow,
        version,
        sampleSize,
        bloomfilterSize,
        timeSliceChoiceMethod,
        historiesEnriched.histories.size,
        reverseSearch
      )
      curResultSerializer.addIndividualResultStats(individualStatLine)
      (queryTimeRQValues + queryTimeIndexTimeSlice, subsetValidationTime, validationTime)
    }
  }

  def queryAll(sample:IndexedSeq[(EnrichedColumnHistory,Int)],
               queryFileName:String,
               multiLevelIndexStructure: MultiLevelIndexStructure,
               queryParameters:TINDParameters):(Double, Double, Double) = {
    val queryAndValidationAndTemporalValidationTimes = if(!allPairsMode){
      sample.map { case (query, queryNumber) =>
        processSingleQuery(curResultSerializer,multiLevelIndexStructure,query,queryParameters,queryFileName,queryNumber,sample.size)
      }
    } else {
      val handler = new ParallelExecutionHandler(sample.size)
      val parallelIOHandler = new ParallelIOHandler(curResultSerializer.targetDir,curResultSerializer.queryFile,curResultSerializer.prefix.get,curResultSerializer.timeSliceChoiceMethod)
      val completed = new AtomicInteger(0)
      val futures = sample.map { case (query, queryNumber) =>
        val f = handler.addAsFuture({
          val curSerializer = parallelIOHandler.getOrCreateNEwResultSerializer()
          val res = processSingleQuery(curSerializer,multiLevelIndexStructure,query,queryParameters,queryFileName,queryNumber,sample.size)
          val curCompleted = completed.incrementAndGet()
          if (curCompleted%500==0) {
            logger.debug(s"Finished $curCompleted (${100*curCompleted / sample.size.toDouble}%)")
          }
          parallelIOHandler.releaseResultSerializer(curSerializer)
          res
        })
        f
      }
      handler.awaitTermination()
      parallelIOHandler.availableResultSerializers.foreach(_.closeAll())
      val results = futures.map(f => {
        val res = Await.result(f, scala.concurrent.duration.Duration.MinusInf)
        res
      })
      results
    }
    val totalQueryTime = queryAndValidationAndTemporalValidationTimes.map(_._1).sum
    val totalIndexValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._2).sum
    val totalTemporalValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._3).sum
    (totalQueryTime, totalIndexValidationTime, totalTemporalValidationTime)

  }

  private def validate(query: EnrichedColumnHistory,queryParamters:TINDParameters, actualCandidates: ArrayBuffer[EnrichedColumnHistory],curResultSerializer: ResultSerializer) = {
    val (trueTemporalINDs, validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query,queryParamters,actualCandidates))
    val truePositiveCount = trueTemporalINDs.size
    curResultSerializer.addTrueTemporalINDs(trueTemporalINDs)
    (validationTime,truePositiveCount)
  }

  def tINDSearch(queryIDsFile:File,
                 timeSliceIndexNumbers:IndexedSeq[Int],
                 queryParameters:TINDParameters,
                 resultSerializer: StandardResultSerializer) = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery(Some(queryIDsFile),timeSliceIndexNumbers,queryParameters,resultSerializer)
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

  def tINDAllPairs(numtimeSliceIndices: Int,
                   queryParameters: TINDParameters,
                   resultSerializer: StandardResultSerializer) = {
    val (_, totalTime) = TimeUtil.executionTimeInMS {
      runDiscovery(None, IndexedSeq(numtimeSliceIndices), queryParameters, resultSerializer)
    }
    TimeUtil.logRuntime(totalTime, "ms", "Total Execution Time")
  }


}
