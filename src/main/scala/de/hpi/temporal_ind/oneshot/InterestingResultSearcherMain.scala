package de.hpi.temporal_ind.oneshot

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.data.ind.{INDCandidate, INDCandidateIDs}
import de.hpi.temporal_ind.discovery.input_data.InputDataManager

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

object InterestingResultSearcherMain extends App with StrictLogging{
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val temporalINDRootPath = args(0)
  val dataPath = args(1)
  val resultDir = args(2)
  new File(resultDir).mkdirs()
  val dataLoader = new InputDataManager(dataPath)
  val discoveredTemporalINDs = new File(temporalINDRootPath)
    .listFiles()
    .filter(_.isDirectory)
    .map(f => f.getAbsolutePath + "/discoveredINDs.jsonl")
    .flatMap(s => INDCandidateIDs.fromJsonObjectPerLineFile(s))
  logger.debug("Loaded ind ids")
  val data = dataLoader.loadData()
    .map(och => ((och.columnHistoryID.pageID,och.id),och))
    .toMap
  logger.debug("Loaded data")
  var validINMANY = 0
  val listOfInteresting = collection.mutable.ArrayBuffer[INDCandidateIDs]()
  val listOfLessInteresting = collection.mutable.ArrayBuffer[INDCandidateIDs]()
  discoveredTemporalINDs.foreach(id => {
    val (left: OrderedColumnHistory, right: OrderedColumnHistory) = getOchs(id)
    if(!left.versionAt(GLOBAL_CONFIG.lastInstant).isDelete && !right.versionAt(GLOBAL_CONFIG.lastInstant).isDelete){
      if(left.versionAt(GLOBAL_CONFIG.lastInstant).values.subsetOf(right.versionAt(GLOBAL_CONFIG.lastInstant).values)){
        validINMANY +=1
      } else {
        listOfInteresting += id
      }
    } else {
      listOfLessInteresting += id
    }
  })

  val prInteresting = new PrintWriter(resultDir + "/interesting.csv")
  val prLessInteresting = new PrintWriter(resultDir + "/lessInteresting.csv")

  def serializeAll(pr: PrintWriter, listOfInteresting: ArrayBuffer[INDCandidateIDs]) = {
    logger.debug(s"Serializing ${listOfInteresting.size}")
    pr.println(INDCandidate.csvSchema)
    listOfInteresting.foreach(ids => {
      val (left,right) = getOchs(ids)
      val lastVersionLeft = left.history.versions.filter(!_._2.isDelete).last._1
      val lastVersionRight = right.history.versions.filter(!_._2.isDelete).last._1
      pr.println(INDCandidate(left,right).toLabelCSVString(lastVersionLeft,Some(lastVersionRight)))
    })
    pr.close()
  }

  serializeAll(prInteresting,listOfInteresting)
  serializeAll(prLessInteresting,listOfLessInteresting)

  private def getOchs(id: INDCandidateIDs) = {
    val left = data((id.lhsPageID, id.lhsColumnID))
    val right = data((id.rhsPageID, id.rhsColumnID))
    (left, right)
  }

  println("total",discoveredTemporalINDs.size)
  println("validInMANY",validINMANY)
  println("nonDeleteAtLastTimestampNotValidAtLatest",listOfInteresting.size)
  println("deleteAtLastTimestamp",listOfLessInteresting.size)



}
