package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.ShifteddRelaxedCustomFunctionTemporalIND

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

class StandardResultSerializer(targetDir:File) extends ResultSerializer{
  def addTrueTemporalINDs(trueTemporalINDs: ArrayBuffer[ShifteddRelaxedCustomFunctionTemporalIND[String]]) =
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


  val resultPR = new PrintWriter(targetDir + "/discoveredINDs.jsonl")
  val basicQueryInfoRow = new PrintWriter(targetDir + "/basicQueryInfo.csv")
  val totalResultsStats = new PrintWriter(targetDir + "/totalStats.csv")
  val individualStats = new PrintWriter(targetDir + "/improvedIndividualStats.csv")
  totalResultsStats.println(TotalResultStats.schema)
  individualStats.println(IndividualResultStats.schema)
  basicQueryInfoRow.println(BasicQueryInfoRow.schema)


}
