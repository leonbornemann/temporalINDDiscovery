package de.hpi.temporal_ind.discovery

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.{AbstractColumnVersion, ColumnHistoryID}
import de.hpi.temporal_ind.data.attribute_history.data.original.{ColumnHistory, OrderedColumnHistory}
import de.hpi.temporal_ind.data.column.data.original.KryoSerializableColumnHistory
import de.hpi.temporal_ind.data.ind.weight_functions.ConstantWeightFunction
import de.hpi.temporal_ind.data.ind.{EpsilonDeltaRelaxedTemporalIND, EpsilonOmegaDeltaRelaxedTemporalIND, INDCandidateIDs, ValidationVariant}
import de.hpi.temporal_ind.discovery.indexing.time_slice_choice.{DynamicWeightedRandomTimeSliceChooser, TimeSliceChooser, WeightedRandomTimeSliceChooser, WeightedShuffledTimestamps}
import de.hpi.temporal_ind.discovery.indexing.{BloomfilterIndex, MultiLevelIndexStructure, MultiTimeSliceIndexStructure, TimeSliceChoiceMethod}
import de.hpi.temporal_ind.discovery.input_data.{ColumnHistoryStorage, EnrichedColumnHistory, InputDataManager, ValuesInTimeWindow}
import de.hpi.temporal_ind.discovery.statistics_and_results.{BasicQueryInfoRow, IndividualResultStats, ResultSerializer, StandardResultSerializer, TotalResultStats}
import de.hpi.temporal_ind.util.TimeUtil
import de.metanome.algorithms.many.bitvectors.BitVector
import org.json4s.scalap.scalasig.ClassFileParser.byte

import java.time.Duration
import java.util
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}
//import org.nustaq.serialization.{FSTConfiguration, FSTObjectInput, FSTObjectOutput}
//import org.nustaq.serialization.util.FSTOutputStream

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, OutputStream, PrintWriter}
import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class TINDSearcher(val dataManager:InputDataManager,
                   val version:String,
                   val subsetValidation:Boolean,
                   val timeSliceChoiceMethod:TimeSliceChoiceMethod.Value,
                   var nThreads:Int,
                   val metaDir:File,
                   val reverseSearch:Boolean) extends StrictLogging{
  def useSubsetOfData(inputSizeFactor: Int) = {
    assert(inputSizeFactor <= historiesEnriched.histories.size)
    this.historiesEnriched = new ColumnHistoryStorage(historiesEnrichedOriginal.histories.take(inputSizeFactor))
    // data loading time is now longer correct now, but that does not matter for the query use-case
  }

  var allPairsMode = false
  var fullMultiIndexStructure:MultiLevelIndexStructure = null
  var historiesEnriched:ColumnHistoryStorage = null
  var historiesEnrichedOriginal:ColumnHistoryStorage = null
  var dataLoadingTimeMS:Double=0.0
  var expectedQueryParameters:TINDParameters = null

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
    fullMultiIndexStructure = buildMultiIndexStructure(historiesEnriched, maxNumTimeSlicIndices)
  }


  var curResultSerializer:StandardResultSerializer = null
  //val parallelIOHandler = new ParallelIOHandler(resultRootDir,queryFile, bloomfilterSize, timeSliceChoiceMethod, seed)

  var seed:Long = 0
  var bloomfilterSize:Int= -1
  var random:Random = null

  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och)))
  }

  def getRequiredValuesetIndex(historiesEnriched: ColumnHistoryStorage) = {
    val func = if(!reverseSearch) (e:EnrichedColumnHistory) => e.allValues else (e:EnrichedColumnHistory) => e.requiredValues(expectedQueryParameters)
    new BloomfilterIndex(historiesEnriched.histories,
      bloomfilterSize,
      func
      )
  }

  def getIndexForTimeSlice(historiesEnriched:ColumnHistoryStorage,lower:Instant, upper:Instant,size:Int=bloomfilterSize) = {
    logger.debug(s"Building Index for [$lower,$upper)")
    val (beginDelta,endDelta) = (lower.minusNanos(expectedQueryParameters.absDeltaInNanos),upper.plusNanos(expectedQueryParameters.absDeltaInNanos))
    new BloomfilterIndex(historiesEnriched.histories,
      size,
      (e:EnrichedColumnHistory) => e.valueSetInWindow(beginDelta,endDelta)
      )
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

//  def getIterableForTimeSliceIndices(historiesEnriched: ColumnHistoryStorage) = {
//    val slices = getTimeSlices(historiesEnriched)
//      .iterator
//      .map { case (begin, end) =>
//        ((begin,end),getIndexForTimeSlice(historiesEnriched,begin,end))
//      }
//    slices
//  }

  def buildTimeSliceIndices(historiesEnriched: ColumnHistoryStorage,indicesToBuild:Int) = {
    val weightedShuffleFile = WeightedShuffledTimestamps.getImportFile(metaDir,seed,timeSliceChoiceMethod)
    val timeSliceChooser = TimeSliceChooser.getChooser(timeSliceChoiceMethod,historiesEnriched,expectedQueryParameters,random,weightedShuffleFile,reverseSearch)

    val slices = collection.mutable.ArrayBuffer[(Instant,Instant)]()
    (0 until indicesToBuild).foreach(_ => {
      val timeSlice = timeSliceChooser.getNextTimeSlice()
      slices += timeSlice
    })
    if (!weightedShuffleFile.exists() && timeSliceChoiceMethod == TimeSliceChoiceMethod.DYNAMIC_WEIGHTED_RANDOM) {
      timeSliceChooser.asInstanceOf[DynamicWeightedRandomTimeSliceChooser].exportAsFile(weightedShuffleFile)
    }
    logger.debug(s"Running Index Build for ${historiesEnriched.histories.size} attributes with time slices: $slices")
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    //concurrent index building to speed up index construction:
    val handler = new ParallelExecutionHandler(slices.size)
    val futures = slices.map{case (begin,end) => {
      handler.addAsFuture({
        val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched, begin, end))
        buildTimes += timeSliceIndexBuild
        ((begin, end), timeSliceIndex)
      })
    }}
    handler.awaitTermination()
    val results = futures.map(f => Await.result(f,scala.concurrent.duration.Duration.MinusInf))
    val indexMap = collection.mutable.TreeMap[(Instant,Instant),BloomfilterIndex]() ++ results
    (indexMap,buildTimes)
  }




  def buildMultiIndexStructure(historiesEnriched: ColumnHistoryStorage,numTimeSliceIndices:Int) = {
    val (indexEntireValueset,requiredValuesIndexBuildTime) = TimeUtil.executionTimeInMS(getRequiredValuesetIndex(historiesEnriched))
    val (timeSliceIndices, timeSliceIndexBuildTimes) = buildTimeSliceIndices(historiesEnriched, numTimeSliceIndices)
    val timeSliceIndexStructure = new MultiTimeSliceIndexStructure(timeSliceIndices, timeSliceIndexBuildTimes)
    new MultiLevelIndexStructure(indexEntireValueset,timeSliceIndexStructure,requiredValuesIndexBuildTime)
  }

  def runDiscovery(queryIDsFile:Option[File],
                   numTimeSliceIndicesList:IndexedSeq[Int],
                   queryParameters:TINDParameters,
                   resultSerializer:StandardResultSerializer) = {
    allPairsMode = queryIDsFile.isEmpty
    curResultSerializer = resultSerializer//new StandardResultSerializer(resultRootDir,queryFile, timeSliceChoiceMethod)
    //time slice values index:
    //old
    //val sample = getRandomSampleOfInputData(historiesEnriched,sampleSize )

    val queries = if(queryIDsFile.isDefined){
      val queryIDs = ColumnHistoryID
        .fromJsonObjectPerLineFile(queryIDsFile.get.getAbsolutePath)
        .toSet
      historiesEnriched
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


  def getRandomSampleOfInputData(historiesEnriched: ColumnHistoryStorage,sampleSize: Int) = {
    random.shuffle(historiesEnriched.histories.zipWithIndex).take(sampleSize)
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

//  def discoverAll(numTimeSliceIndices: Int,nThreads:Int) = {
//    val (_, totalTime) = TimeUtil.executionTimeInMS {
//      runDiscovery(None, IndexedSeq(numTimeSliceIndices),nThreads)
//    }
//    TimeUtil.logRuntime(totalTime, "ms", "Total Execution Time")
//  }

}
