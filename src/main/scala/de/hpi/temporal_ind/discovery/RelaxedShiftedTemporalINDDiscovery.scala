package de.hpi.temporal_ind.discovery

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.column.data.original.{ColumnHistory, KryoSerializableColumnHistory, OrderedColumnHistory, OrderedColumnVersionList, ValidationVariant}
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.metanome.algorithms.many.bitvectors.BitVector
import org.json4s.scalap.scalasig.ClassFileParser.byte

import java.util
import scala.collection.mutable
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
                                         val random:Random = new Random(13)) extends StrictLogging{

  def version = if(interactiveIndexBuilding) versionParam + "_interactive" else versionParam

  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos*epsilon).toLong


  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och,absoluteEpsilonNanos)))
  }

  def getIndexForEntireValueset(historiesEnriched: ColumnHistoryStorage) = {
    new BloomfilterIndex(historiesEnriched.histories,
      bloomfilterSize,
      (e:EnrichedColumnHistory) => e.allValues,
      (e:EnrichedColumnHistory) => Seq(e.requiredValues))
  }

  def getIndexForTimeSlice(historiesEnriched:ColumnHistoryStorage,lower:Instant, upper:Instant,size:Int=bloomfilterSize) = {
    val (beginDelta,endDelta) = (lower.minusNanos(deltaInNanos),upper.plusNanos(deltaInNanos))
    new BloomfilterIndex(historiesEnriched.histories,
      size,
      (e:EnrichedColumnHistory) => e.valueSetInWindow(beginDelta,endDelta),
      (e:EnrichedColumnHistory) => e.och.versionsInWindowNew(lower,upper).map(_._2.values).toSeq)
  }

  def validateCandidates(query:EnrichedColumnHistory,candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]) = {
    candidatesRequiredValues.map(refCandidate => {
      //new TemporalShifted
      new ShifteddRelaxedCustomFunctionTemporalIND[String](query.och, refCandidate.och, deltaInNanos, absoluteEpsilonNanos, new ConstantWeightFunction(), ValidationVariant.FULL_TIME_PERIOD)
    }).filter(_.isValid)
  }

  def buildTimeSliceIndices(historiesEnriched: ColumnHistoryStorage,indicesToBuild:Int) = {
    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
    logger.debug("Found ",allSlices.size," time slices")
    val slices = random.shuffle(allSlices)
      .take(indicesToBuild)
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    val indexMap = collection.mutable.TreeMap[(Instant,Instant),BloomfilterIndex]() ++ slices.map{case (begin,end) => {
      val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched,begin,end))
      buildTimes+=timeSliceIndexBuild
      ((begin,end),timeSliceIndex)
    }}
    (indexMap,buildTimes)
  }

  def interactiveTimeSliceIndicesBuilding(historiesEnriched: ColumnHistoryStorage) = {
    var done = false
    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
    logger.debug("Found " + allSlices.size + " time slices")
    val slices = random.shuffle(allSlices)
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
    val (indexEntireValueset,requiredValuesIndexBuildTime) = TimeUtil.executionTimeInMS(getIndexForEntireValueset(historiesEnriched))
    val (timeSliceIndices, timeSliceIndexBuildTimes) = if (interactiveIndexBuilding) {
      interactiveTimeSliceIndicesBuilding(historiesEnriched)
    } else {
      buildTimeSliceIndices(historiesEnriched, numTimeSliceIndices)
    }
    new MultiLevelIndexStructure(indexEntireValueset,timeSliceIndices,requiredValuesIndexBuildTime,timeSliceIndexBuildTimes)
  }

  def runDiscovery(sampleSize:Int, numTimeSliceIndicesList:IndexedSeq[Int]) = {
    val beforePreparation = System.nanoTime()
    val histories = dataManager.loadData()
    val historiesEnriched = enrichWithHistory(histories)
    //required values index:
    val afterPreparation = System.nanoTime()
    val dataLoadingTimeMS = (afterPreparation - beforePreparation) / 1000000.0
    val multiIndexStructure = buildMultiIndexStructure(historiesEnriched,numTimeSliceIndicesList.max)
    //time slice values index:
    //query all:
    val sample = random.shuffle(historiesEnriched.histories.zipWithIndex).take(sampleSize)
    numTimeSliceIndicesList.foreach(numTimeSliceIndices => {
      logger.debug(s"Processing $numTimeSliceIndices")
      val curIndex = multiIndexStructure.limitTimeSliceIndices(numTimeSliceIndices)
      val (totalQueryTime,totalSubsetValidationTime,totalTemporalValidationTime) =  queryAll(sample,curIndex)
      val totalResultSTatsLine = TotalResultStats(numTimeSliceIndices,dataLoadingTimeMS,curIndex.requiredValuesIndexBuildTime,curIndex.totalTimeSliceIndexBuildTime,totalQueryTime,totalSubsetValidationTime,totalTemporalValidationTime)
      resultSerializer.addTotalResultStats(totalResultSTatsLine)
    })
    resultSerializer.closeAll()
  }



  private def queryAll(sample:IndexedSeq[(EnrichedColumnHistory,Int)],
                       multiLevelIndexStructure: MultiLevelIndexStructure) = {
    val queryAndValidationAndTemporalValidationTimes = sample.map{case (query,queryNumber) => {
      val (candidatesRequiredValues, queryTimeRQValues) = multiLevelIndexStructure.queryRequiredValuesIndex( query)
      val (curCandidates, queryTimeIndexTimeSlice) = multiLevelIndexStructure.queryTimeSliceIndices(query,candidatesRequiredValues)
      val numCandidatesAfterIndexQuery = curCandidates.count()-1
      //validate curCandidates after all candidates have already been pruned
      val subsetValidationTime = if(subsetValidation){
        multiLevelIndexStructure.validateContainments(query,curCandidates)
      } else {
        0.0
      }
      val candidateLineages = multiLevelIndexStructure
        .bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
        .filter(_ != query)
      val numCandidatesAfterSubsetValidation = candidateLineages.size
      val (validationTime,truePositiveCount) = validate(query, candidateLineages)
      val validationStatRow = BasicQueryInfoRow(queryNumber,
        query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant),
        query.och.pageID,
        query.och.tableId,
        query.och.id)
      resultSerializer.addBasicQueryInfoRow(validationStatRow)
      val avgVersionsPerTimeSliceWindow = multiLevelIndexStructure.timeSliceIndices
        .keys
        .map{case (s,e) => query.och.versionsInWindow(s,e).size}
        .sum / multiLevelIndexStructure.timeSliceIndices.size.toDouble
      val individualStatLine = IndividualResultStats(queryNumber,
        multiLevelIndexStructure.timeSliceIndices.size,
        queryTimeRQValues+queryTimeIndexTimeSlice,
        subsetValidationTime,
        validationTime,
        numCandidatesAfterIndexQuery,
        numCandidatesAfterSubsetValidation,
        truePositiveCount,
        avgVersionsPerTimeSliceWindow,
        version
        ) //TODO: append this to a writer,
      resultSerializer.addIndividualResultStats(individualStatLine)
      (queryTimeRQValues+queryTimeIndexTimeSlice,subsetValidationTime,validationTime)
    }}
    val totalQueryTime = queryAndValidationAndTemporalValidationTimes.map(_._1).sum
    val totalIndexValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._2).sum
    val totalTemporalValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._3).sum
    (totalQueryTime,totalIndexValidationTime,totalTemporalValidationTime)
  }


  private def validate(query: EnrichedColumnHistory, actualCandidates: ArrayBuffer[EnrichedColumnHistory]) = {
    val (trueTemporalINDs, validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query,actualCandidates))
    val truePositiveCount = trueTemporalINDs.size
    resultSerializer.addTrueTemporalINDs(trueTemporalINDs)
    (validationTime,truePositiveCount)
  }

  def discover(timeSliceIndexNumbers:IndexedSeq[Int],sampleSize:Int) = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery(sampleSize,timeSliceIndexNumbers)
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

}
