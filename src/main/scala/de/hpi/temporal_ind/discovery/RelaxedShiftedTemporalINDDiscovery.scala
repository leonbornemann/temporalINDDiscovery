package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.original.{OrderedColumnHistory, ValidationVariant}
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.metanome.algorithms.many.bitvectors.BitVector
import org.json4s.scalap.scalasig.ClassFileParser.byte

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
                                         val version:String,
                                         val random:Random = new Random(13)) extends StrictLogging{

  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos*epsilon).toLong
  val resultPR = new PrintWriter(targetDir + "/discoveredINDs.jsonl")
  val indexQueryStatsPR = new PrintWriter(targetDir + "/indexQueryStats.csv")
  val validationStatsPR = new PrintWriter(targetDir + "/validationStats.csv")
  val statsPROtherTimes = new PrintWriter(targetDir + "/discoveryStatTimes.csv")
  val totalResultsStats = new PrintWriter(targetDir + "/totalStats.csv")
  val individualStats = new PrintWriter(targetDir + "/improvedIndividualStats.csv")
  totalResultsStats.println(TotalResultStats.schema)
  individualStats.println(IndividualResultStats.schema)
  indexQueryStatsPR.println(QueryStatRow.schema)
  validationStatsPR.println(ValidationStatRow.schema)
  //val kryo = new Kryo()

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
      (e:EnrichedColumnHistory) => e.allValues,
      (e:EnrichedColumnHistory) => e.requiredValues)
  }

  def getIndexForTimeSlice(historiesEnriched:ColumnHistoryStorage,lower:Instant, upper:Instant) = {
    val (beginDelta,endDelta) = (lower.minusNanos(deltaInNanos),upper.plusNanos(deltaInNanos))
    new BloomfilterIndex(historiesEnriched.histories,
      (e:EnrichedColumnHistory) => e.och.valuesInWindow(beginDelta,endDelta),
      (e:EnrichedColumnHistory) => e.och.valuesInWindow(lower,upper))
  }

  def validateCandidates(query:EnrichedColumnHistory,candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]) = {
    candidatesRequiredValues.map(refCandidate => {
      //new TemporalShifted
      new ShifteddRelaxedCustomFunctionTemporalIND[String](query.och, refCandidate.och, deltaInNanos, absoluteEpsilonNanos, new ConstantWeightFunction(), ValidationVariant.FULL_TIME_PERIOD)
    }).filter(_.isValid)
  }

//  def serializeAsBinary(histories: IndexedSeq[OrderedColumnHistory], path: String) = {
//    val os = new Output(new FileOutputStream(new File(path)))
//    kryo.writeClassAndObject(os,histories.toBuffer)
//    os.close()
//  }

//  def loadAsBinary(path:String) = {
//    val is = new Input(new FileInputStream(path))
//    val res = kryo.readClassAndObject(is)
//      .asInstanceOf[collection.mutable.Buffer[OrderedColumnHistory]]
//    is.close()
//    res.toIndexedSeq
//  }

  def buildTimeSliceIndices(historiesEnriched: ColumnHistoryStorage, statsPROtherTimes: PrintWriter,indicesToBuild:Int) = {
    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
    val slices = random.shuffle(allSlices)
      .take(indicesToBuild)
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    val indexMap = slices.map{case (begin,end) => {
      val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched,begin,end))
      statsPROtherTimes.println(s"Time Slice Index Build,$timeSliceIndexBuild")
      buildTimes+=timeSliceIndexBuild
      ((begin,end),timeSliceIndex)
    }}.toMap
    (indexMap,buildTimes)
  }

//  def testBinaryLoading(histories:IndexedSeq[OrderedColumnHistory]) = {
//    serializeAsBinary(histories, targetFileBinary)
//    println(histories.size)
//    val (historiesFromBinary,timeLoadingBinary) = TimeUtil.executionTimeInMS(loadAsBinary(targetFileBinary))
//    statsPROtherTimes.println(s"Data Loading Binary,$timeLoadingBinary")
//    println(s"Data Loading Binary,$timeLoadingBinary")
//    assert(historiesFromBinary.size == histories.size)
//    historiesFromBinary.zip(histories).foreach { case (h1, h2) => {
//      assert(h1.id == h2.id)
//      assert(h1.tableId == h2.tableId)
//      assert(h1.pageID == h2.pageID)
//      assert(h1.pageTitle == h2.pageTitle)
//      assert(h1.history.versions == h2.history.versions)
//    }
//    }
//    println("Check successful, binary file intact")
//  }

  def runDiscovery(sampleSize:Int,numTimeSliceIndicesList:IndexedSeq[Int]) = {
    val beforePreparation = System.nanoTime()
    val (histories,timeLoadingJson) = TimeUtil.executionTimeInMS(loadData())
    statsPROtherTimes.println(s"Data Loading Json,$timeLoadingJson")
    println(s"Data Loading Json,$timeLoadingJson")
    //testBinaryLoading(histories)
    val historiesEnriched = enrichWithHistory(histories)
    //required values index:
    val afterPreparation = System.nanoTime()
    val dataLoadingTimeMS = (afterPreparation - beforePreparation) / 1000000.0
    val (indexEntireValueset,requirecValuesIndexBuildTime) = TimeUtil.executionTimeInMS(getIndexForEntireValueset(historiesEnriched))
    statsPROtherTimes.println(s"Index Build,$requirecValuesIndexBuildTime")
    //time slice values index:
    val (timeSliceIndices,indexBuildTimes) = buildTimeSliceIndices(historiesEnriched,statsPROtherTimes,numTimeSliceIndicesList.max)
    statsPROtherTimes.close()
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
    indexQueryStatsPR.close()
    statsPROtherTimes.close()
    validationStatsPR.close()
  }

  def queryTimeSliceIndices(candidatesRequiredValues: BitVector[_], timeSliceIndices: Map[(Instant, Instant), BloomfilterIndex], query: EnrichedColumnHistory, queryNumber: Int) = {
    var curCandidates = candidatesRequiredValues
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    val queryAndValidationTimes = timeSliceIndices
      .zipWithIndex
      .map { case (((begin, end), index), indexOrder) => {
        val candidateCountBefore = curCandidates.count()
        val (candidatesIndexSlice, queryTimeSliceTime,timeSliceValidationTime) = index.queryWithBitVectorResult(query,
          Some(curCandidates), false) //TODO: change validate
        curCandidates = candidatesIndexSlice
        val candidateCountAfter = curCandidates.count()
        queryTimesSlices += queryTimeSliceTime
        val queryStatRow = new QueryStatRow(queryNumber, query, queryTimeSliceTime, "Time Slice", candidateCountBefore, candidateCountAfter, Some(begin), Some(end), Some(indexOrder))
        indexQueryStatsPR.println(queryStatRow.toCSVLine())
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
      //validate curCandidates after all candidates have already been pruned
      val (_,subsetValidationTime) = TimeUtil.executionTimeInMS({
        entireValuesetIndex.validateContainment(query,curCandidates)
        timeSliceIndices.foreach(index => index._2.validateContainment(query,curCandidates))
      })
      val candidateLineages = entireValuesetIndex
        .bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
        .filter(_ != query)
      val (validationTime,truePositiveCount) = validate(query, candidateLineages)
      val validationStatRow = ValidationStatRow(queryNumber,
        query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant),
        query.och.pageID,
        query.och.tableId,
        query.och.id,
        validationTime,
        candidateLineages.size,
        truePositiveCount,
        version)
      validationStatsPR.println(validationStatRow.toCSVLine)
      val individualStatLine = IndividualResultStats(queryNumber,
        timeSliceIndices.size,
        queryTimeRQValues+queryTimeIndexTimeSlice,
        subsetValidationTime,
        validationTime,
        candidateLineages.size,
        truePositiveCount
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
    val queryStatRow = new QueryStatRow(queryNumber, query, queryTime, "Required Values", historiesEnriched.histories.size * (historiesEnriched.histories.size + 1) / 2, candidatesRequiredValues.size(), None, None, Some(-1))
    indexQueryStatsPR.println(queryStatRow.toCSVLine())
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
    statsPROtherTimes.println(s"Data Loading,$timeDataLoading")
    histories
  }

  def discover(timeSliceIndexNumbers:IndexedSeq[Int]) = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery(1000,timeSliceIndexNumbers)
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

}
