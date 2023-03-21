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

class RelaxedShiftedTemporalINDDiscovery(val sourceDirs: IndexedSeq[File],
                                         val targetFileBinary:String,
                                         val targetDir: File,
                                         val epsilon: Double,
                                         val deltaInNanos: Long,
                                         val versionParam:String,
                                         val subsetValidation:Boolean,
                                         val bloomfilterSize:Int,
                                         val interactiveIndexBuilding:Boolean,
                                         val random:Random = new Random(13)) extends StrictLogging{

  def version = if(interactiveIndexBuilding) versionParam + "_interactive" else versionParam

  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos*epsilon).toLong
  val resultPR = new PrintWriter(targetDir + "/discoveredINDs.jsonl")
  val basicQueryInfoRow = new PrintWriter(targetDir + "/basicQueryInfo.csv")
  val totalResultsStats = new PrintWriter(targetDir + "/totalStats.csv")
  val individualStats = new PrintWriter(targetDir + "/improvedIndividualStats.csv")
  totalResultsStats.println(TotalResultStats.schema)
  individualStats.println(IndividualResultStats.schema)
  basicQueryInfoRow.println(BasicQueryInfoRow.schema)
  val kryo = new Kryo();



  def loadHistories() =
    sourceDirs
      .flatMap(sourceDir => OrderedColumnHistory
        .readFromFiles(sourceDir)
        .toIndexedSeq)

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

  def serializeAsBinary(histories: IndexedSeq[OrderedColumnHistory], path: String) = {
    val os = new Output(new FileOutputStream(new File(path)))
    val historySerializable = histories.map(och => och.toKryoSerializableColumnHistory)
    val list = new util.ArrayList[KryoSerializableColumnHistory]()
    historySerializable.foreach(list.add(_))
    kryo.writeClassAndObject(os,list)
    //os.write(kryo.toBytesWithClass(histories.toBuffer))//.writeClassAndObject(os,histories.toBuffer)
    os.close()
  }

  def loadAsBinary(path:String) = {
    val is = new Input(new FileInputStream(path))
    val res = kryo.readClassAndObject(is)
      .asInstanceOf[java.util.List[KryoSerializableColumnHistory]]
    is.close()
    res.asScala.map(k => OrderedColumnHistory.fromKryoSerializableColumnHistory(k)).toIndexedSeq
  }

  def buildTimeSliceIndices(historiesEnriched: ColumnHistoryStorage,indicesToBuild:Int) = {
    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
    val slices = random.shuffle(allSlices)
      .take(indicesToBuild)
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    val indexMap = slices.map{case (begin,end) => {
      val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched,begin,end))
      buildTimes+=timeSliceIndexBuild
      ((begin,end),timeSliceIndex)
    }}.toMap
    (indexMap,buildTimes)
  }

  def testBinaryLoading(histories:IndexedSeq[OrderedColumnHistory]) = {
    serializeAsBinary(histories, targetFileBinary)
    val (historiesFromBinary,timeLoadingBinary) = TimeUtil.executionTimeInMS(loadAsBinary(targetFileBinary))
    println(s"Data Loading Binary,$timeLoadingBinary")
    assert(historiesFromBinary.size == histories.size)
    historiesFromBinary.zip(histories).foreach { case (h1, h2) => {
      assert(h1.id == h2.id)
      assert(h1.tableId == h2.tableId)
      assert(h1.pageID == h2.pageID)
      assert(h1.pageTitle == h2.pageTitle)
      assert(h1.history.versions.isInstanceOf[collection.SortedMap[Instant,AbstractColumnVersion[String]]])
      assert(h1.history.versions == h2.history.versions)
    }
    }
    println("Check successful, binary file intact")
  }

  def interactiveTimeSliceIndicesBuilding(historiesEnriched: ColumnHistoryStorage) = {
    var done = false
    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
    val slices = random.shuffle(allSlices)
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    while(!done){
      println("Please enter <number of indices to build,bloomfilter-size>, q to quit")
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
        println("Done with indexing, please confirm continuation by hitting any key")
        scala.io.StdIn.readLine()
      }
    }
    (null, buildTimes)
  }

  def runDiscovery(sampleSize:Int, numTimeSliceIndicesList:IndexedSeq[Int]) = {
    val beforePreparation = System.nanoTime()
    val (histories,timeLoadingJson) = TimeUtil.executionTimeInMS(loadData())
    println(s"Data Loading Json,$timeLoadingJson")
    testBinaryLoading(histories)
    val historiesEnriched = enrichWithHistory(histories)
    //required values index:
    val afterPreparation = System.nanoTime()
    val dataLoadingTimeMS = (afterPreparation - beforePreparation) / 1000000.0
    val (indexEntireValueset,requirecValuesIndexBuildTime) = TimeUtil.executionTimeInMS(getIndexForEntireValueset(historiesEnriched))
    //time slice values index:
    val (timeSliceIndices,indexBuildTimes) = if(interactiveIndexBuilding){
      interactiveTimeSliceIndicesBuilding(historiesEnriched)
    } else {
      buildTimeSliceIndices(historiesEnriched,numTimeSliceIndicesList.max)
    }
    //query all:
    val sample = random.shuffle(historiesEnriched.histories.zipWithIndex).take(sampleSize)
    numTimeSliceIndicesList.foreach(numTimeSliceIndices => {
      logger.debug(s"Processing $numTimeSliceIndices")
      val (totalQueryTime,totalSubsetValidationTime,totalTemporalValidationTime) =  qeuryAll(historiesEnriched,sample,indexEntireValueset,timeSliceIndices.take(numTimeSliceIndices))
      val totalResultSTatsLine = TotalResultStats(numTimeSliceIndices,dataLoadingTimeMS,requirecValuesIndexBuildTime,indexBuildTimes.take(numTimeSliceIndices).sum,totalQueryTime,totalSubsetValidationTime,totalTemporalValidationTime)
      totalResultsStats.println(totalResultSTatsLine.toCSV)
      totalResultsStats.flush()
    })
    individualStats.close()
    totalResultsStats.close()
    resultPR.close()
    basicQueryInfoRow.close()
  }

  def queryTimeSliceIndices(candidatesRequiredValues: BitVector[_], timeSliceIndices: Map[(Instant, Instant), BloomfilterIndex], query: EnrichedColumnHistory, queryNumber: Int) = {
    var curCandidates = candidatesRequiredValues
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    val queryAndValidationTimes = timeSliceIndices
      .zipWithIndex
      .map { case (((begin, end), index), indexOrder) => {
        val (candidatesIndexSlice, queryTimeSliceTime,timeSliceValidationTime) = index.queryWithBitVectorResult(query,
          Some(curCandidates), false)
        curCandidates = candidatesIndexSlice
        queryTimesSlices += queryTimeSliceTime
        (queryTimeSliceTime,timeSliceValidationTime)
      }
      }
    val queryTimeTotal = queryAndValidationTimes.map(_._1).sum
    val validationTimeTotal = queryAndValidationTimes.map(_._2).sum
    (curCandidates,queryTimeTotal)
  }

  private def qeuryAll(historiesEnriched: ColumnHistoryStorage,
                       sample:IndexedSeq[(EnrichedColumnHistory,Int)],
                       entireValuesetIndex:BloomfilterIndex,
                       timeSliceIndices:Map[(Instant,Instant),BloomfilterIndex]) = {
    val queryAndValidationAndTemporalValidationTimes = sample.map{case (query,queryNumber) => {
      val (candidatesRequiredValues, queryTimeRQValues) = queryRequiredValuesIndex(historiesEnriched, entireValuesetIndex, query, queryNumber)
      val (curCandidates, queryTimeIndexTimeSlice) = queryTimeSliceIndices(candidatesRequiredValues,timeSliceIndices,query,queryNumber)
      val numCandidatesAfterIndexQuery = curCandidates.count()-1
      //validate curCandidates after all candidates have already been pruned
      val subsetValidationTime = if(subsetValidation){
        val (_, subsetValidationTime) = TimeUtil.executionTimeInMS({
          entireValuesetIndex.validateContainment(query, curCandidates)
          timeSliceIndices.foreach(index => index._2.validateContainment(query, curCandidates))
        })
        subsetValidationTime
      } else {
        0.0
      }
      val candidateLineages = entireValuesetIndex
        .bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
        .filter(_ != query)
      val numCandidatesAfterSubsetValidation = candidateLineages.size
      val (validationTime,truePositiveCount) = validate(query, candidateLineages)
      val validationStatRow = BasicQueryInfoRow(queryNumber,
        query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant),
        query.och.pageID,
        query.och.tableId,
        query.och.id)
      basicQueryInfoRow.println(validationStatRow.toCSVLine)
      val avgVersionsPerTimeSliceWindow = timeSliceIndices
        .keys
        .map{case (s,e) => query.och.versionsInWindow(s,e).size}
        .sum / timeSliceIndices.size.toDouble
      val individualStatLine = IndividualResultStats(queryNumber,
        timeSliceIndices.size,
        queryTimeRQValues+queryTimeIndexTimeSlice,
        subsetValidationTime,
        validationTime,
        numCandidatesAfterIndexQuery,
        numCandidatesAfterSubsetValidation,
        truePositiveCount,
        avgVersionsPerTimeSliceWindow,
        version
        ) //TODO: append this to a writer,
      individualStats.println(individualStatLine.toCSVLine)
      (queryTimeRQValues+queryTimeIndexTimeSlice,subsetValidationTime,validationTime)
    }}
    val totalQueryTime = queryAndValidationAndTemporalValidationTimes.map(_._1).sum
    val totalIndexValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._2).sum
    val totalTemporalValidationTime = queryAndValidationAndTemporalValidationTimes.map(_._3).sum
    (totalQueryTime,totalIndexValidationTime,totalTemporalValidationTime)
  }

  private def queryRequiredValuesIndex(historiesEnriched: ColumnHistoryStorage,
                                       entireValuesetIndex: BloomfilterIndex,
                                       query: EnrichedColumnHistory,
                                       queryNumber: Int) = {
    val (candidatesRequiredValues, queryTime,validationTime) = entireValuesetIndex.queryWithBitVectorResult(query,
      None,
      false)
    (candidatesRequiredValues, queryTime)
  }

  private def validate(query: EnrichedColumnHistory, actualCandidates: ArrayBuffer[EnrichedColumnHistory]) = {
    val (trueTemporalINDs, validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query,actualCandidates))
    val truePositiveCount = trueTemporalINDs.size
    trueTemporalINDs.foreach(c => c.toCandidateIDs.appendToWriter(resultPR))
    (validationTime,truePositiveCount)
  }

  private def loadData() = {
    val (histories, timeDataLoading) = TimeUtil.executionTimeInMS(loadHistories())
    histories
  }

  def discover(timeSliceIndexNumbers:IndexedSeq[Int],sampleSize:Int) = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery(sampleSize,timeSliceIndexNumbers)
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

}
