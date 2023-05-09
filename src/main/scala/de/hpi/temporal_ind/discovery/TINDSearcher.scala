package de.hpi.temporal_ind.discovery

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.{AbstractColumnVersion, ColumnHistoryID}
import de.hpi.temporal_ind.data.attribute_history.data.original.{ColumnHistory, OrderedColumnHistory}
import de.hpi.temporal_ind.data.column.data.original.KryoSerializableColumnHistory
import de.hpi.temporal_ind.data.ind.weight_functions.ConstantWeightFunction
import de.hpi.temporal_ind.data.ind.{EpsilonDeltaRelaxedTemporalIND, EpsilonOmegaDeltaRelaxedTemporalIND, ValidationVariant}
import de.hpi.temporal_ind.discovery.indexing.time_slice_choice.{TimeSliceChooser, WeightedRandomTimeSliceChooser}
import de.hpi.temporal_ind.discovery.indexing.{BloomfilterIndex, MultiLevelIndexStructure, MultiTimeSliceIndexStructure, TimeSliceChoiceMethod}
import de.hpi.temporal_ind.discovery.input_data.{ColumnHistoryStorage, EnrichedColumnHistory, InputDataManager, ValuesInTimeWindow}
import de.hpi.temporal_ind.discovery.statistics_and_results.{BasicQueryInfoRow, IndividualResultStats, ResultSerializer, StandardResultSerializer, TotalResultStats}
import de.hpi.temporal_ind.util.TimeUtil
import de.metanome.algorithms.many.bitvectors.BitVector
import org.json4s.scalap.scalasig.ClassFileParser.byte

import java.time.Duration
import java.util
import java.util.concurrent.Semaphore
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
                   val queryFile:File,
                   val resultRootDir:File,
                   val expectedQueryParameters:TINDParameters,
                   val version:String,
                   val subsetValidation:Boolean,
                   val bloomfilterSize:Int,
                   val timeSliceChoiceMethod:TimeSliceChoiceMethod.Value,
                   val seed:Long,
                   val nThreads:Int,
                   val metaDir:File) extends StrictLogging{

  val singleResultSerializer = new StandardResultSerializer(resultRootDir,queryFile,bloomfilterSize, timeSliceChoiceMethod, seed)
  val parallelIOHandler = new ParallelIOHandler(resultRootDir,queryFile, bloomfilterSize, timeSliceChoiceMethod, seed)

  val random:Random = new Random(seed)

  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och)))
  }

  def getRequiredValuesetIndex(historiesEnriched: ColumnHistoryStorage) = {
    new BloomfilterIndex(historiesEnriched.histories,
      bloomfilterSize,
      (e:EnrichedColumnHistory) => e.allValues,
      //(e:EnrichedColumnHistory) => Seq(new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant,e.requiredValues)),
      )
  }

  def getIndexForTimeSlice(historiesEnriched:ColumnHistoryStorage,lower:Instant, upper:Instant,size:Int=bloomfilterSize) = {
    logger.debug(s"Building Index for [$lower,$upper)")
    val (beginDelta,endDelta) = (lower.minusNanos(expectedQueryParameters.absDeltaInNanos),upper.plusNanos(expectedQueryParameters.absDeltaInNanos))
    new BloomfilterIndex(historiesEnriched.histories,
      size,
      (e:EnrichedColumnHistory) => e.valueSetInWindow(beginDelta,endDelta),
      //(e:EnrichedColumnHistory) => e.och.versionsInWindowNew(lower,upper).map(t => new ValuesInTimeWindow(lower,upper,t._2.values)).toSeq,
      )
  }

  def validateCandidates(query:EnrichedColumnHistory,
                         queryParameters:TINDParameters,
                         candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]):collection.IndexedSeq[EpsilonOmegaDeltaRelaxedTemporalIND[String]] = {
    val batchSize = GLOBAL_CONFIG.PARALLEL_TIND_VALIDATION_BATCH_SIZE
    if(candidatesRequiredValues.size<batchSize || nThreads==1){
      val res = candidatesRequiredValues.map(refCandidate => {
        //new TemporalShifted
        new EpsilonOmegaDeltaRelaxedTemporalIND[String](query.och, refCandidate.och, queryParameters, ValidationVariant.FULL_TIME_PERIOD)
      }).filter(_.isValid)
      res
    } else {
      //do it in parallel
      val batches = candidatesRequiredValues.grouped(5).toIndexedSeq
      val handler = new ParallelExecutionHandler(batches.size)
      val futures = batches.map {batch =>
        val f = handler.addAsFuture(batch.map(refCandidate => {
          new EpsilonOmegaDeltaRelaxedTemporalIND[String](query.och, refCandidate.och, queryParameters, ValidationVariant.FULL_TIME_PERIOD)
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
    val weightedShuffleFile = new File(metaDir.getAbsolutePath + s"/$seed.json")
    val timeSliceChooser = TimeSliceChooser.getChooser(timeSliceChoiceMethod,historiesEnriched,expectedQueryParameters,random,weightedShuffleFile)
    if(!weightedShuffleFile.exists() && timeSliceChoiceMethod == TimeSliceChoiceMethod.WEIGHTED_RANDOM){
      timeSliceChooser.asInstanceOf[WeightedRandomTimeSliceChooser].exportAsFile(weightedShuffleFile)
    }
    val slices = collection.mutable.ArrayBuffer[(Instant,Instant)]()
    (0 until indicesToBuild).foreach(_ => {
      val timeSlice = timeSliceChooser.getNextTimeSlice()
      slices += timeSlice
    })
    logger.debug(s"Running Index Build with time slices: $slices")
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

  def runDiscovery(queryIDs:Option[Set[ColumnHistoryID]], numTimeSliceIndicesList:IndexedSeq[Int], queryParameters:TINDParameters) = {
    val (historiesEnriched: ColumnHistoryStorage, dataLoadingTimeMS: Double) = loadData()
    val multiIndexStructure = buildMultiIndexStructure(historiesEnriched,numTimeSliceIndicesList.max)
    //time slice values index:
    //old
    //val sample = getRandomSampleOfInputData(historiesEnriched,sampleSize )
    val queries = if(queryIDs.isDefined){
      historiesEnriched
        .histories
        .filter(h => queryIDs.get.contains(h.och.columnHistoryID))
        .zipWithIndex
    } else {
      historiesEnriched.histories.zipWithIndex
    }
    logger.debug(s"Processing ${queries.size} queries")
    numTimeSliceIndicesList.foreach(numTimeSliceIndices => {
      logger.debug(s"Processing numTimeSliceIndices=$numTimeSliceIndices")
      val curIndex = multiIndexStructure.limitTimeSliceIndices(numTimeSliceIndices)
      val (totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime) = queryAll(queries, curIndex,queryParameters)
      val totalResultSTatsLine = TotalResultStats(version, seed, queryIDs.size, bloomfilterSize, timeSliceChoiceMethod, numTimeSliceIndices, dataLoadingTimeMS, curIndex.requiredValuesIndexBuildTime, curIndex.totalTimeSliceIndexBuildTime, totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime)
      singleResultSerializer.addTotalResultStats(totalResultSTatsLine)
    })
    singleResultSerializer.closeAll()
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

  def queryAll(sample:IndexedSeq[(EnrichedColumnHistory,Int)],
               multiLevelIndexStructure: MultiLevelIndexStructure,
               queryParameters:TINDParameters,
               executeInParallel:Boolean=false):(Double, Double, Double) = {
    if(!executeInParallel){
      val queryAndValidationAndTemporalValidationTimes = sample.map { case (query, queryNumber) => {
        val (candidatesRequiredValues, queryTimeRQValues) = multiLevelIndexStructure.queryRequiredValuesIndex(query,queryParameters)
        val (curCandidates, queryTimeIndexTimeSlice) = multiLevelIndexStructure.queryTimeSliceIndices(query,queryParameters, candidatesRequiredValues)
        val numCandidatesAfterIndexQuery = curCandidates.count() - 1
        //validate curCandidates after all candidates have already been pruned
        val subsetValidationTime = if (subsetValidation) {
          multiLevelIndexStructure.validateContainments(query, queryParameters,curCandidates)
        } else {
          0.0
        }
        val candidateLineages = multiLevelIndexStructure
          .bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
          .filter(_ != query)
        val numCandidatesAfterSubsetValidation = candidateLineages.size
        val (validationTime, truePositiveCount) = validate(query,queryParameters, candidateLineages)
        val validationStatRow = BasicQueryInfoRow(queryNumber,
          query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant),
          query.och.pageID,
          query.och.tableId,
          query.och.id)
        singleResultSerializer.addBasicQueryInfoRow(validationStatRow)
        val avgVersionsPerTimeSliceWindow = multiLevelIndexStructure
          .timeSliceIndices
          .timeSliceIndices
          .keys
          .map { case (s, e) => query.och.versionsInWindow(s, e).size }
          .sum / multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.size.toDouble
        val individualStatLine = IndividualResultStats(queryNumber,
          multiLevelIndexStructure.timeSliceIndices.timeSliceIndices.size,
          queryTimeRQValues + queryTimeIndexTimeSlice,
          subsetValidationTime,
          validationTime,
          numCandidatesAfterIndexQuery,
          numCandidatesAfterSubsetValidation,
          truePositiveCount,
          avgVersionsPerTimeSliceWindow,
          version,
          sample.size,
          bloomfilterSize,
          timeSliceChoiceMethod
        )
        singleResultSerializer.addIndividualResultStats(individualStatLine)
        (queryTimeRQValues + queryTimeIndexTimeSlice, subsetValidationTime, validationTime)
      }
      }
      val totalQueryTime = queryAndValidationAndTemporalValidationTimes.map(_._1).sum
      val totalIndexValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._2).sum
      val totalTemporalValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._3).sum
      (totalQueryTime, totalIndexValidationTime, totalTemporalValidationTime)
    } else {
      throw new AssertionError("Not yet implemented")
      //TODO: parallelize:
      val batchSize = 100

//      sample.foreach(batch => {
//        val f = Future{
//
//        }
//      })
      null
    }

  }


  private def validate(query: EnrichedColumnHistory,queryParamters:TINDParameters, actualCandidates: ArrayBuffer[EnrichedColumnHistory]) = {
    val (trueTemporalINDs, validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query,queryParamters,actualCandidates))
    val truePositiveCount = trueTemporalINDs.size
    singleResultSerializer.addTrueTemporalINDs(trueTemporalINDs)
    (validationTime,truePositiveCount)
  }

  def discoverForSample(queryIDs:Set[ColumnHistoryID], timeSliceIndexNumbers:IndexedSeq[Int],queryParameters:TINDParameters) = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery(Some(queryIDs),timeSliceIndexNumbers,queryParameters)
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

//  def discoverAll(numTimeSliceIndices: Int,nThreads:Int) = {
//    val (_, totalTime) = TimeUtil.executionTimeInMS {
//      runDiscovery(None, IndexedSeq(numTimeSliceIndices),nThreads)
//    }
//    TimeUtil.logRuntime(totalTime, "ms", "Total Execution Time")
//  }

}
