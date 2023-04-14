package de.hpi.temporal_ind.discovery

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, ColumnHistoryID}
import de.hpi.temporal_ind.data.column.data.original.{ColumnHistory, KryoSerializableColumnHistory, OrderedColumnHistory, OrderedColumnVersionList, SimpleCounter, ValidationVariant}
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.{BloomfilterIndex, MultiLevelIndexStructure, MultiTimeSliceIndexStructure, TimeSliceChoiceMethod}
import de.hpi.temporal_ind.discovery.input_data.{ColumnHistoryStorage, EnrichedColumnHistory, InputDataManager, ValuesInTimeWindow}
import de.hpi.temporal_ind.discovery.statistics_and_results.{BasicQueryInfoRow, IndividualResultStats, ResultSerializer, TotalResultStats}
import de.metanome.algorithms.many.bitvectors.BitVector
import org.json4s.scalap.scalasig.ClassFileParser.byte

import java.util
import java.util.concurrent.Semaphore
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}
//import org.nustaq.serialization.{FSTConfiguration, FSTObjectInput, FSTObjectOutput}
//import org.nustaq.serialization.util.FSTOutputStream

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, OutputStream, PrintWriter}
import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class RelaxedShiftedTemporalINDDiscovery(val dataManager:InputDataManager,
                                         val resultSerializer:ResultSerializer,
                                         val epsilon: Double,
                                         val deltaInNanos: Long,
                                         val versionParam:String,
                                         val subsetValidation:Boolean,
                                         val bloomfilterSize:Int,
                                         val interactiveIndexBuilding:Boolean,
                                         val timeSliceChoiceMethod:TimeSliceChoiceMethod.Value,
                                         val useViolationTracking:Boolean,
                                         val seed:Long) extends StrictLogging{


  val random:Random = new Random(seed)
  def version = if(interactiveIndexBuilding) versionParam + "_interactive" else versionParam

  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos*epsilon).toLong


  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och,absoluteEpsilonNanos)))
  }

  def getRequiredValuesetIndex(historiesEnriched: ColumnHistoryStorage) = {
    new BloomfilterIndex(historiesEnriched.histories,
      bloomfilterSize,
      (e:EnrichedColumnHistory) => e.allValues,
      (e:EnrichedColumnHistory) => Seq(new ValuesInTimeWindow(GLOBAL_CONFIG.earliestInstant,GLOBAL_CONFIG.lastInstant,e.requiredValues)),
      absoluteEpsilonNanos)
  }

  def getIndexForTimeSlice(historiesEnriched:ColumnHistoryStorage,lower:Instant, upper:Instant,size:Int=bloomfilterSize) = {
    val (beginDelta,endDelta) = (lower.minusNanos(deltaInNanos),upper.plusNanos(deltaInNanos))
    new BloomfilterIndex(historiesEnriched.histories,
      size,
      (e:EnrichedColumnHistory) => e.valueSetInWindow(beginDelta,endDelta),
      (e:EnrichedColumnHistory) => e.och.versionsInWindowNew(lower,upper).map(t => new ValuesInTimeWindow(lower,upper,t._2.values)).toSeq,
      absoluteEpsilonNanos)
  }

  def validateCandidates(query:EnrichedColumnHistory,candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]) = {
    candidatesRequiredValues.map(refCandidate => {
      //new TemporalShifted
      new ShifteddRelaxedCustomFunctionTemporalIND[String](query.och, refCandidate.och, deltaInNanos, absoluteEpsilonNanos, new ConstantWeightFunction(), ValidationVariant.FULL_TIME_PERIOD)
    }).filter(_.isValid)
  }

  def getIterableForTimeSliceIndices(historiesEnriched: ColumnHistoryStorage) = {
    val slices = getTimeSlices(historiesEnriched)
      .iterator
      .map { case (begin, end) =>
        ((begin,end),getIndexForTimeSlice(historiesEnriched,begin,end))
      }
    slices
  }

  def buildTimeSliceIndices(historiesEnriched: ColumnHistoryStorage,indicesToBuild:Int) = {
    val slices = getTimeSlices(historiesEnriched)
      .take(indicesToBuild)
    logger.debug(s"Running Index Build with time slices: $slices")
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    val indexMap = collection.mutable.TreeMap[(Instant,Instant),BloomfilterIndex]() ++ slices.map{case (begin,end) => {
      val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched,begin,end))
      buildTimes+=timeSliceIndexBuild
      ((begin,end),timeSliceIndex)
    }}
    (indexMap,buildTimes)
  }

  def getTimeSlices(historiesEnriched:ColumnHistoryStorage):IndexedSeq[(Instant,Instant)] = {
    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
    if(timeSliceChoiceMethod==TimeSliceChoiceMethod.RANDOM){
      random.shuffle(allSlices)
    } else {
      val timeSliceToOccurrences = collection.mutable.TreeMap[Instant, (Instant, SimpleCounter)]() ++ allSlices.map(s => (s._1, (s._2, SimpleCounter())))
      historiesEnriched
        .histories
        .foreach(e => e.och.addPresenceForTimeRanges(timeSliceToOccurrences))
      if (timeSliceChoiceMethod == TimeSliceChoiceMethod.BESTX) {
        //using simple mutable counter is more efficient than immutable int since we don't haven to reassign in the map
        timeSliceToOccurrences
          .toIndexedSeq
          .sortBy(_._2._2.count)
          .map(t => (t._1,t._2._1))
      } else {
        //do weighted random selection
        new WeightedRandomShuffler(random).shuffle(timeSliceToOccurrences.map(t => ((t._1,t._2._1),t._2._2.count)))
      }
    }
  }

  def interactiveTimeSliceIndicesBuilding(historiesEnriched: ColumnHistoryStorage) = {
    var done = false
    val slices = getTimeSlices(historiesEnriched)
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    while(!done){
      println("Please enter <number of indices to build,bloomfilter-size>, q to quit")
      System.gc()
      val line = scala.io.StdIn.readLine()
      if(line == "q"){
        done = true
      } else {
        val numberOfIndices = line.split(",")(0).toInt
        val bloomfilterSizeNew = line.split(",")(1).toInt
        val curIndexMap = slices.take(numberOfIndices).map { case (begin, end) => {
          val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched, begin, end,bloomfilterSizeNew))
          buildTimes += timeSliceIndexBuild
          ((begin, end), timeSliceIndex)
        }
        }.toMap
        println(curIndexMap.size,"Done with indexing, please confirm continuation by hitting any key")
        scala.io.StdIn.readLine()
        println(curIndexMap.size)
      }
    }
    (null, buildTimes)
  }

  def buildMultiIndexStructure(historiesEnriched: ColumnHistoryStorage,numTimeSliceIndices:Int) = {
    val (indexEntireValueset,requiredValuesIndexBuildTime) = TimeUtil.executionTimeInMS(getRequiredValuesetIndex(historiesEnriched))
    val (timeSliceIndices, timeSliceIndexBuildTimes) = if (interactiveIndexBuilding) {
      interactiveTimeSliceIndicesBuilding(historiesEnriched)
    } else {
      buildTimeSliceIndices(historiesEnriched, numTimeSliceIndices)
    }
    val timeSliceIndexStructure = new MultiTimeSliceIndexStructure(timeSliceIndices, timeSliceIndexBuildTimes, useViolationTracking, absoluteEpsilonNanos)
    new MultiLevelIndexStructure(indexEntireValueset,timeSliceIndexStructure,requiredValuesIndexBuildTime)
  }

  def runDiscovery(queryIDs:Option[Set[ColumnHistoryID]], numTimeSliceIndicesList:IndexedSeq[Int],nThreads:Int=1) = {
    val (historiesEnriched: ColumnHistoryStorage, dataLoadingTimeMS: Double) = loadData()
    val multiIndexStructure = buildMultiIndexStructure(historiesEnriched,numTimeSliceIndicesList.max)
    //time slice values index:
    //old
    //val sample = getRandomSampleOfInputData(historiesEnriched,sampleSize )
    val sample = if(queryIDs.isDefined){
      historiesEnriched
        .histories
        .filter(h => queryIDs.get.contains(h.och.columnHistoryID))
        .zipWithIndex
    } else {
      historiesEnriched.histories.zipWithIndex
    }
    logger.debug(s"Processing ${sample.size} queries")
    numTimeSliceIndicesList.foreach(numTimeSliceIndices => {
      logger.debug(s"Processing $numTimeSliceIndices")
      val curIndex = multiIndexStructure.limitTimeSliceIndices(numTimeSliceIndices)
      val (totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime) = queryAll(sample, curIndex,nThreads)
      val totalResultSTatsLine = TotalResultStats(version, seed, queryIDs.size, bloomfilterSize, useViolationTracking, timeSliceChoiceMethod, numTimeSliceIndices, dataLoadingTimeMS, curIndex.requiredValuesIndexBuildTime, curIndex.totalTimeSliceIndexBuildTime, totalQueryTime, totalSubsetValidationTime, totalTemporalValidationTime)
      resultSerializer.addTotalResultStats(totalResultSTatsLine)
    })
    resultSerializer.closeAll()
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
               nThreads:Int=1):(Double, Double, Double) = {
    if(nThreads==1){
      val queryAndValidationAndTemporalValidationTimes = sample.map { case (query, queryNumber) => {
        val (candidatesRequiredValues, queryTimeRQValues) = multiLevelIndexStructure.queryRequiredValuesIndex(query)
        val (curCandidates, queryTimeIndexTimeSlice) = multiLevelIndexStructure.queryTimeSliceIndices(query, candidatesRequiredValues)
        val numCandidatesAfterIndexQuery = curCandidates.count() - 1
        //validate curCandidates after all candidates have already been pruned
        val subsetValidationTime = if (subsetValidation) {
          multiLevelIndexStructure.validateContainments(query, curCandidates)
        } else {
          0.0
        }
        val candidateLineages = multiLevelIndexStructure
          .bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
          .filter(_ != query)
        val numCandidatesAfterSubsetValidation = candidateLineages.size
        val (validationTime, truePositiveCount) = validate(query, candidateLineages)
        val validationStatRow = BasicQueryInfoRow(queryNumber,
          query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant),
          query.och.pageID,
          query.och.tableId,
          query.och.id)
        resultSerializer.addBasicQueryInfoRow(validationStatRow)
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
          useViolationTracking,
          timeSliceChoiceMethod
        )
        resultSerializer.addIndividualResultStats(individualStatLine)
        (queryTimeRQValues + queryTimeIndexTimeSlice, subsetValidationTime, validationTime)
      }
      }
      val totalQueryTime = queryAndValidationAndTemporalValidationTimes.map(_._1).sum
      val totalIndexValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._2).sum
      val totalTemporalValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._3).sum
      (totalQueryTime, totalIndexValidationTime, totalTemporalValidationTime)
    } else {
      //TODO: parallelize:
      val batchSize = 100
      val batches = sample.sliding(100,100)
//      sample.foreach(batch => {
//        val f = Future{
//
//        }
//      })
      null
    }

  }


  private def validate(query: EnrichedColumnHistory, actualCandidates: ArrayBuffer[EnrichedColumnHistory]) = {
    val (trueTemporalINDs, validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query,actualCandidates))
    val truePositiveCount = trueTemporalINDs.size
    resultSerializer.addTrueTemporalINDs(trueTemporalINDs)
    (validationTime,truePositiveCount)
  }

  def discoverForSample(queryIDs:Set[ColumnHistoryID], timeSliceIndexNumbers:IndexedSeq[Int]) = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery(Some(queryIDs),timeSliceIndexNumbers)
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

  def discoverAll(numTimeSliceIndices: Int,nThreads:Int) = {
    val (_, totalTime) = TimeUtil.executionTimeInMS {
      runDiscovery(None, IndexedSeq(numTimeSliceIndices),nThreads)
    }
    TimeUtil.logRuntime(totalTime, "ms", "Total Execution Time")
  }

}
