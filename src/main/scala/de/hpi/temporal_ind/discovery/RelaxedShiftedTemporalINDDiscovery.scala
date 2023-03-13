package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.original.{OrderedColumnHistory, ValidationVariant}
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.metanome.algorithms.many.bitvectors.BitVector
import org.json4s.scalap.scalasig.ClassFileParser.byte
import org.nustaq.serialization.{FSTConfiguration, FSTObjectInput, FSTObjectOutput}
import org.nustaq.serialization.util.FSTOutputStream

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
  indexQueryStatsPR.println(QueryStatRow.schema)
  validationStatsPR.println(ValidationStatRow.schema)

  def loadHistories() =
    sourceDirs
      .flatMap(sourceDir => OrderedColumnHistory
        .readFromFiles(sourceDir)
        .toIndexedSeq)

  def enrichWithHistory(histories: IndexedSeq[OrderedColumnHistory]) = {
    new ColumnHistoryStorage(histories.map(och => new EnrichedColumnHistory(och,absoluteEpsilonNanos)))
  }

  def getIndexForEntireValueset(historiesEnriched: ColumnHistoryStorage) = {
    new BloomfilterIndex(historiesEnriched.histories,((e:EnrichedColumnHistory) => e.allValues))
  }

  def getIndexForTimeSlice(historiesEnriched:ColumnHistoryStorage,lower:Instant,upper:Instant) = {
    new BloomfilterIndex(historiesEnriched.histories,(e:EnrichedColumnHistory) => e.och.valuesInWindow(lower,upper))
  }

  def validateCandidates(query:EnrichedColumnHistory,candidatesRequiredValues: ArrayBuffer[EnrichedColumnHistory]) = {
    candidatesRequiredValues.map(refCandidate => {
      //new TemporalShifted
      new ShifteddRelaxedCustomFunctionTemporalIND[String](query.och, refCandidate.och, deltaInNanos, absoluteEpsilonNanos, new ConstantWeightFunction(), ValidationVariant.FULL_TIME_PERIOD)
    }).filter(_.isValid)
  }

  def serializeAsBinary(histories: IndexedSeq[OrderedColumnHistory], path: String) = {
    val os = new FileOutputStream(new File(path))
    val out = new FSTObjectOutput(os)
    out.writeObject(histories)
    //histories.foreach(h => out.writeObject(h))
    //out.writeObject(histories.last)
    out.close // required !
  }

  def loadAsBinary(path:String) = {
    val is = new FileInputStream(path)
    val in = new FSTObjectInput(is)
    val res = in.readObject().asInstanceOf[IndexedSeq[OrderedColumnHistory]]
    in.close()
    res
//    var done = false
//    val buffer = collection.mutable.ArrayBuffer[OrderedColumnHistory]()
//    while(!done){
//      val obj:OrderedColumnHistory = in.readObject().asInstanceOf[OrderedColumnHistory]
//      println(buffer.size)
//      if (buffer.size == 4627) {
//        println()
//      }
//      if(obj==null){
//        done=true
//      } else {
//        buffer += obj
//      }
//    }
//    buffer.toIndexedSeq
  }

  def buildTimeSliceIndices(historiesEnriched: ColumnHistoryStorage, statsPROtherTimes: PrintWriter) = {
    val indicesToBuild=3
    val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
    val slices = random.shuffle(allSlices)
      .take(indicesToBuild)
    slices.map{case (begin,end) => {
      val (beginDelta,endDelta) = (begin.minusNanos(deltaInNanos),end.plusNanos(deltaInNanos))
      val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched,beginDelta,endDelta))
      statsPROtherTimes.println(s"Time Slice Index Build,$timeSliceIndexBuild")
      ((beginDelta,endDelta),timeSliceIndex)
    }}.toMap
  }

  def testBinaryLoading(histories:IndexedSeq[OrderedColumnHistory]) = {
    serializeAsBinary(histories, targetFileBinary)
    println(histories.size)
    val (historiesFromBinary,timeLoadingBinary) = TimeUtil.executionTimeInMS(loadAsBinary(targetFileBinary))
    statsPROtherTimes.println(s"Data Loading Binary,$timeLoadingBinary")
    println(s"Data Loading Binary,$timeLoadingBinary")
    assert(historiesFromBinary.size == histories.size)
    historiesFromBinary.zip(histories).foreach { case (h1, h2) => {
      assert(h1.id == h2.id)
      assert(h1.tableId == h2.tableId)
      assert(h1.pageID == h2.pageID)
      assert(h1.pageTitle == h2.pageTitle)
      assert(h1.history.versions == h2.history.versions)
    }
    }
    println("Check successful, binary file intact")
  }

  def runDiscovery() = {
    val (histories,timeLoadingJson) = TimeUtil.executionTimeInMS(loadData())
    statsPROtherTimes.println(s"Data Loading Json,$timeLoadingJson")
    println(s"Data Loading Json,$timeLoadingJson")
    testBinaryLoading(histories)
    val historiesEnriched = enrichWithHistory(histories)
    //required values index:
    val (indexEntireValueset,timeIndexBuild) = TimeUtil.executionTimeInMS(getIndexForEntireValueset(historiesEnriched))
    statsPROtherTimes.println(s"Index Build,$timeIndexBuild")
    //time slice values index:
    val timeSliceIndices = buildTimeSliceIndices(historiesEnriched,statsPROtherTimes)
    statsPROtherTimes.close()
    //query all:
    qeuryAll(historiesEnriched,indexEntireValueset,timeSliceIndices)
    resultPR.close()
    indexQueryStatsPR.close()
    statsPROtherTimes.close()
    validationStatsPR.close()
  }

  def queryTimeSliceIndices(candidatesRequiredValues: BitVector[_], timeSliceIndices: Map[(Instant, Instant), BloomfilterIndex], query: EnrichedColumnHistory, queryNumber: Int) = {
    var curCandidates = candidatesRequiredValues
    val queryTimesSlices = collection.mutable.ArrayBuffer[Double]()
    timeSliceIndices
      .zipWithIndex
      .foreach { case (((begin, end), index), indexOrder) => {
        val candidateCountBefore = curCandidates.count()
        val (candidatesIndexSlice, queryTimeSlice) = TimeUtil.executionTimeInMS(index.queryWithBitVectorResult(query,
          (e: EnrichedColumnHistory) => e.och.valuesInWindow(begin, end),
          Some(curCandidates), true))
        curCandidates = candidatesIndexSlice
        val candidateCountAfter = curCandidates.count()
        queryTimesSlices += queryTimeSlice
        val queryStatRow = new QueryStatRow(queryNumber, query, queryTimeSlice, "Time Slice", candidateCountBefore, candidateCountAfter, Some(begin), Some(end), Some(indexOrder))
        indexQueryStatsPR.println(queryStatRow.toCSVLine())
      }
      }
    curCandidates
  }

  private def qeuryAll(historiesEnriched: ColumnHistoryStorage,
                       entireValuesetIndex:BloomfilterIndex,
                       timeSliceIndices:Map[(Instant,Instant),BloomfilterIndex]): Unit = {
    historiesEnriched.histories.zipWithIndex.foreach{case (query,queryNumber) => {
      val candidatesRequiredValues: BitVector[_] = queryRequiredValuesIndex(historiesEnriched, entireValuesetIndex, query, queryNumber)
      val curCandidates = queryTimeSliceIndices(candidatesRequiredValues,timeSliceIndices,query,queryNumber)
      val candidateLineages = entireValuesetIndex.bitVectorToColumns(curCandidates) //does not matter which index transforms it back because all have them in the same order
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
    }}
  }

  private def queryRequiredValuesIndex(historiesEnriched: ColumnHistoryStorage, entireValuesetIndex: BloomfilterIndex, query: EnrichedColumnHistory, queryNumber: Int) = {
    val (candidatesRequiredValues, queryTime) = TimeUtil.executionTimeInMS(entireValuesetIndex.queryWithBitVectorResult(query,
      ((e: EnrichedColumnHistory) => e.requiredValues),
      None,
      true))
    val queryStatRow = new QueryStatRow(queryNumber, query, queryTime, "Required Values", historiesEnriched.histories.size * (historiesEnriched.histories.size + 1) / 2, candidatesRequiredValues.size(), None, None, Some(-1))
    indexQueryStatsPR.println(queryStatRow.toCSVLine())
    candidatesRequiredValues
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

  def discover() = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery()
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

}
