package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.ind.ShifteddRelaxedCustomFunctionTemporalIND
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod.TimeSliceChoiceMethod

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

class StandardResultSerializer(targetDir:File,
                               bloomFilterSize:Int,
                               timeSliceChoiceMethod:TimeSliceChoiceMethod.Value,
                               seed:Long,
                               subDirectoryNum:Option[Int]=None) extends ResultSerializer{
  def addTrueTemporalINDs(trueTemporalINDs: Iterable[ShifteddRelaxedCustomFunctionTemporalIND[String]]) =
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

  val filePrefix = s"${bloomFilterSize}_${timeSliceChoiceMethod}_${seed}_"
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
