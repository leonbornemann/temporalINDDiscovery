package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.{BloomfilterIndex, MultiLevelIndexStructure, MultiTimeSliceIndexStructure, TimeSliceChoiceMethod}
import de.hpi.temporal_ind.discovery.input_data.{EnrichedColumnHistory, InputDataManager}
import de.hpi.temporal_ind.discovery.statistics_and_results.{BasicQueryInfoRow, IndividualResultStats, StandardResultSerializer, TotalResultStats}
import de.hpi.temporal_ind.util.TimeUtil
import de.metanome.algorithms.many.bitvectors.BitVector

import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.Await
import scala.util.Random

class BaselineTemporalINDDiscovery(dataLoader: InputDataManager, subsetValidation: Boolean,numThreads:Int)
  extends TINDDiscoveryAlgorithm(dataLoader, false, numThreads) with StrictLogging{

  var queryParameters: TINDParameters = null
  var curResultSerializer: StandardResultSerializer = null
  var seed: Long = 0
  var bloomfilterSize: Int = -1
  var random: Random = null
  var multiLevelIndexStructure:MultiLevelIndexStructure = null

  val completePruningFailTimes = collection.mutable.ArrayBuffer[(Double,Double,Double)]()

  def notPrunable(query: EnrichedColumnHistory): Boolean = {
    val timestamps = multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.map(_._1._1).toIndexedSeq
    val timestampsWithData = timestamps.map(t => if(query.och.versionAt(t).isDelete) 0 else 1).sum
    timestampsWithData <= (queryParameters.absoluteEpsilon / TimeUtil.nanosPerDay)
  }


  def processSingleQuery(curResultSerializer: StandardResultSerializer, query: EnrichedColumnHistory, queryFileName: String, queryNumber: Int, size: Int) = {
    if (query.och.lifetimeIsBelowThreshold(queryParameters)) {
      //just skip this - it is contained in all others!
      logger.debug(s"Skipping query $queryNumber, because it is contained in all other queries")
      (0.0, 0.0, 0.0)
    } else if (completePruningFailTimes.size==10 && notPrunable(query)) {
      logger.debug("Skipping candidate because not prunable,")
      val times = (completePruningFailTimes.map(t => t._1).sum / completePruningFailTimes.size,completePruningFailTimes.map(t => t._2).sum / completePruningFailTimes.size,completePruningFailTimes.map(t => t._3).sum / completePruningFailTimes.size)
      val avgVersionsPerTimeSliceWindow = multiLevelIndexStructure
        .timeSliceIndices
        .timeSliceIndices
        .keys
        .map { case (s, e) => query.och.versionsInWindow(s, e).size }
        .sum / multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.size.toDouble
      val individualStatLine = IndividualResultStats(queryNumber,
        queryFileName,
        queryParameters,
        queryParameters,
        seed,
        multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.size,
        times._1,
        times._2,
        times._3,
        historiesEnriched.histories.size,
        historiesEnriched.histories.size,
        -1,
        avgVersionsPerTimeSliceWindow,
        "0.99",
        historiesEnriched.histories.size,
        bloomfilterSize,
        TimeSliceChoiceMethod.RANDOM,
        historiesEnriched.histories.size,
        false
      )
      curResultSerializer.addIndividualResultStats(individualStatLine)
      times
    } else {
      val candidates:BitVector[_] = multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.head._2.many.allOnes.copy()
      val (curCandidates, queryTimeIndexTimeSlice) = multiLevelIndexStructure.queryTimeSliceIndices(query, queryParameters, candidates, false)
      val numCandidatesAfterIndexQuery = curCandidates.count() - 1
      //validate curCandidates after all candidates have already been pruned
      val subsetValidationTime = if (subsetValidation) {
        multiLevelIndexStructure.validateContainments(query, queryParameters, curCandidates, false)
      } else {
        0.0
      }
      val candidateLineages = multiLevelIndexStructure
        .bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
        .filter(_ != query)
      val numCandidatesAfterSubsetValidation = candidateLineages.size
      val (validationTime, truePositiveCount) = validate(query, queryParameters, candidateLineages)
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
        queryParameters,
        queryParameters,
        seed,
        multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.size,
        queryTimeIndexTimeSlice,
        subsetValidationTime,
        validationTime,
        numCandidatesAfterIndexQuery,
        numCandidatesAfterSubsetValidation,
        truePositiveCount,
        avgVersionsPerTimeSliceWindow,
        "0.99",
        historiesEnriched.histories.size,
        bloomfilterSize,
        TimeSliceChoiceMethod.RANDOM,
        historiesEnriched.histories.size,
        false
      )
      curResultSerializer.addIndividualResultStats(individualStatLine)
      if(notPrunable(query)){
        completePruningFailTimes.append((queryTimeIndexTimeSlice, subsetValidationTime, validationTime))
      }
      (queryTimeIndexTimeSlice, subsetValidationTime, validationTime)
    }
  }

  private def validate(query: EnrichedColumnHistory, queryParamters: TINDParameters, actualCandidates: collection.mutable.ArrayBuffer[EnrichedColumnHistory]) = {
    val (trueTemporalINDs, validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query, queryParamters, actualCandidates))
    val truePositiveCount = trueTemporalINDs.size
    curResultSerializer.addTrueTemporalINDs(trueTemporalINDs)
    (validationTime, truePositiveCount)
  }

  def queryAll(sample: IndexedSeq[(EnrichedColumnHistory, Int)],
               queryFileName: String): (Double, Double, Double) = {
    val queryAndValidationAndTemporalValidationTimes = sample.map { case (query, queryNumber) =>
        processSingleQuery(curResultSerializer, query, queryFileName, queryNumber, sample.size)
      }
    val totalQueryTime = queryAndValidationAndTemporalValidationTimes.map(_._1).sum
    val totalIndexValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._2).sum
    val totalTemporalValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._3).sum
    (totalQueryTime, totalIndexValidationTime, totalTemporalValidationTime)

  }

  def tINDSearch(queryFile: File, indicesToUse: Int, resultSerializer: StandardResultSerializer) = {
    curResultSerializer = resultSerializer
    val queries = getQueries(queryFile)
    logger.debug(s"Processing numTimeSliceIndices=$indicesToUse")
    multiLevelIndexStructure = multiLevelIndexStructure.limitTimeSliceIndices(indicesToUse)
    val (totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime) = queryAll(queries, queryFile.getName)
    val totalResultSTatsLine = TotalResultStats("0.99", queryParameters, queryParameters, seed, queryFile.getName, queries.size, bloomfilterSize, TimeSliceChoiceMethod.RANDOM, indicesToUse, dataLoadingTimeMS, -1, -1, totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime, historiesEnriched.histories.size, false)
    curResultSerializer.addTotalResultStats(totalResultSTatsLine)
    curResultSerializer.closeAll()
  }

  def buildIndicesWithSeed(numIndices: Int, seed: Long, bloomFilterSize: Int, expectedQueryParameters: TINDParameters) = {
    this.queryParameters = expectedQueryParameters
    this.seed = seed
    this.random = new Random(seed)
    this.bloomfilterSize = bloomFilterSize
    System.gc()
    val chosenTimestamps = random.shuffle(GLOBAL_CONFIG.ALL_DAYS).take(numIndices)
    val slices = chosenTimestamps.map(i => {
      val begin = i
      val end = i.plus(1,ChronoUnit.DAYS)
      (begin,end)
    })
    val handler = new ParallelExecutionHandler(slices.size)
    val indexBuilder = new TINDIndexBuilder(new File(""), seed, TimeSliceChoiceMethod.RANDOM, expectedQueryParameters, random, false, bloomfilterSize)
    val buildTimes = collection.mutable.ArrayBuffer[Double]()
    val futures = slices.map { case (begin, end) => {
      handler.addAsFuture({
        val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(indexBuilder.getIndexForTimeSlice(historiesEnriched, begin, end))
        buildTimes += timeSliceIndexBuild
        ((begin, end), timeSliceIndex)
      })
    }
    }
    handler.awaitTermination()
    val results = futures.map(f => Await.result(f, scala.concurrent.duration.Duration.MinusInf))
    val indexMap = collection.mutable.TreeMap[(Instant, Instant), BloomfilterIndex]() ++ results

    val timeSliceIndexStructure = new MultiTimeSliceIndexStructure(indexMap, buildTimes)
    multiLevelIndexStructure = new MultiLevelIndexStructure(None, timeSliceIndexStructure, 0.0)

  }


}
