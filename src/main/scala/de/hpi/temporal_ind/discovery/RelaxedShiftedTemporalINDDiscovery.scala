package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.original.{OrderedColumnHistory, ValidationVariant}
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import org.json4s.scalap.scalasig.ClassFileParser.byte
import org.nustaq.serialization.{FSTConfiguration, FSTObjectInput, FSTObjectOutput}
import org.nustaq.serialization.util.FSTOutputStream

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, OutputStream, PrintWriter}
import scala.collection.mutable.ArrayBuffer

class RelaxedShiftedTemporalINDDiscovery(sourceDirs: IndexedSeq[File],
                                         targetFileBinary:String,
                                         targetDir: File,
                                         epsilon: Double,
                                         deltaInNanos: Long,
                                         version:String) extends StrictLogging{

  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos*epsilon).toLong


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

  def runDiscovery() = {
    val resultPR = new PrintWriter(targetDir + "/discoveredINDs.jsonl")
    val statsPRCSV = new PrintWriter(targetDir + "/discoveryStats.csv")
    val statsPRJson = new PrintWriter(targetDir + "/discoveryStats.jsonl")
    val statsPROtherTimes = new PrintWriter(targetDir + "/discoveryStatTimes.csv")
    statsPRCSV.println(DiscoveryStatRow.schema)
    val (histories,timeDataLoading) = TimeUtil.executionTimeInMS(loadHistories())
    statsPROtherTimes.println(s"Data Loading,$timeDataLoading")
    serializeAsBinary(histories,targetFileBinary)
    println(histories.size)
    val historiesFromBinary = loadAsBinary(targetFileBinary)
    assert(historiesFromBinary.size==histories.size)
    historiesFromBinary.zip(histories).foreach{case (h1,h2) => {
      assert(h1.id==h2.id)
      assert(h1.tableId==h2.tableId)
      assert(h1.pageID==h2.pageID)
      assert(h1.pageTitle==h2.pageTitle)
      assert(h1.history.versions==h2.history.versions)
    }}
    println("Check successful, binary file intact")
    val historiesEnriched = enrichWithHistory(histories)
    val (bloomFilterIndexEntireValueset,timeIndexBuild) = TimeUtil.executionTimeInMS(getIndexForEntireValueset(historiesEnriched))
    statsPROtherTimes.println(s"Index Build,$timeIndexBuild")
    statsPROtherTimes.close()
    //query all:
    historiesEnriched.histories.foreach(query => {
      //TODO: make query return the bitVector once we do more filtering
      val (candidatesRequiredValues,queryTime) = TimeUtil.executionTimeInMS(bloomFilterIndexEntireValueset.query(query,((e:EnrichedColumnHistory) => e.requiredValues)))
      val actualCandidates = candidatesRequiredValues
        .filter(candidateRef => {
          val requiredValues = query.requiredValues
          val allValues = candidateRef.allValues
          requiredValues.subsetOf(allValues)
        })
      val falsePositivesFromMANY = candidatesRequiredValues.size - actualCandidates.size
      if(!actualCandidates.contains(query)){
        println(s"Weird for query ${query.och.compositeID} - not contained in actual candidates")
      }
      val (trueTemporalINDs,validationTime) = TimeUtil.executionTimeInMS(validateCandidates(query,actualCandidates.filter(c => c!=query)))
      val truePositiveCount = trueTemporalINDs.size
      val falsePositiveCountFROMTemporal = actualCandidates.size-truePositiveCount
      if(falsePositiveCountFROMTemporal<0)
        println()
      trueTemporalINDs.foreach(c => c.toCandidateIDs.appendToWriter(resultPR))
      val discoveryStatRow = DiscoveryStatRow.fromEnrichedColumnHistory(query,queryTime,validationTime,falsePositivesFromMANY,falsePositiveCountFROMTemporal,truePositiveCount,version)
      discoveryStatRow.appendToWriter(statsPRJson)
      statsPRCSV.println(discoveryStatRow.toCSVLine)
    })
    resultPR.close()
    statsPRCSV.close()
    statsPRJson.close()
    statsPROtherTimes.close()
  }

  def discover() = {
    val (_,totalTime) = TimeUtil.executionTimeInMS{
      runDiscovery()
    }
    TimeUtil.logRuntime(totalTime,"ms","Total Execution Time")
  }

}
