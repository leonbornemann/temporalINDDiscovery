package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.ind.EpsilonOmegaDeltaRelaxedTemporalIND
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod.TimeSliceChoiceMethod

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

class StandardResultSerializer(val targetDir:File,
                               val queryFile:File,
                               val timeSliceChoiceMethod:TimeSliceChoiceMethod.Value,
                               val prefix:Option[String]=None,
                               val subDirectoryNum:Option[Int]=None) extends ResultSerializer{
  def addTrueTemporalINDs(trueTemporalINDs: Iterable[EpsilonOmegaDeltaRelaxedTemporalIND[String]]) =
    trueTemporalINDs.foreach(c => c.toCandidateIDs.appendToWriter(resultPR))

  def addIndividualResultStats(individualStatLine: IndividualResultStats) = individualStats.println(individualStatLine.toCSVLine)

  def addBasicQueryInfoRow(validationStatRow: BasicQueryInfoRow) = basicQueryInfoRow.println(validationStatRow.toCSVLine)

  def closeAll() = {
    individualStats.close()
    totalResultsStats.close()
    resultPR.close()
    basicQueryInfoRow.close()
  }

  def addTotalResultStats(totalResultSTatsLine: TotalResultStats) = {
    totalResultsStats.println(totalResultSTatsLine.toCSV)
    totalResultsStats.flush()
  }

  val filePrefix = s"${prefix.getOrElse("")}_${queryFile.getName}_${timeSliceChoiceMethod}"
  val subDir = if(subDirectoryNum.isDefined) s"/${subDirectoryNum.get}/" else ""
  val dir = new File(targetDir + s"/${filePrefix}/$subDir")
  dir.mkdirs()
  val resultPR = new PrintWriter(dir.getAbsolutePath + s"/discoveredINDs.jsonl")
  val basicQueryInfoRow = new PrintWriter(dir.getAbsolutePath + s"/basicQueryInfo.csv")
  val totalResultsStats = new PrintWriter(dir.getAbsolutePath + s"/totalStats.csv")
  val individualStats = new PrintWriter(dir.getAbsolutePath + s"/improvedIndividualStats.csv")
  totalResultsStats.println(TotalResultStats.schema)
  individualStats.println(IndividualResultStats.schema)
  basicQueryInfoRow.println(BasicQueryInfoRow.schema)


}
