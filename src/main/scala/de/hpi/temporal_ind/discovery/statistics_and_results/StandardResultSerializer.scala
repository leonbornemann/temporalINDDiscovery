package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.ind.ShifteddRelaxedCustomFunctionTemporalIND
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod.TimeSliceChoiceMethod

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

class StandardResultSerializer(targetDir:File,
                               bloomFilterSize:Int,
                               enableEarlyAbort:Boolean,
                               sampleSize:Int,
                               timeSliceChoiceMethod:TimeSliceChoiceMethod.Value) extends ResultSerializer{
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

  val filePrefix = s"${bloomFilterSize}_${enableEarlyAbort}_${sampleSize}_${timeSliceChoiceMethod}_"
  val resultPR = new PrintWriter(targetDir + s"/${filePrefix}_discoveredINDs.jsonl")
  val basicQueryInfoRow = new PrintWriter(targetDir + s"/${filePrefix}_basicQueryInfo.csv")
  val totalResultsStats = new PrintWriter(targetDir + s"/${filePrefix}_totalStats.csv")
  val individualStats = new PrintWriter(targetDir + s"/${filePrefix}_improvedIndividualStats.csv")
  totalResultsStats.println(TotalResultStats.schema)
  individualStats.println(IndividualResultStats.schema)
  basicQueryInfoRow.println(BasicQueryInfoRow.schema)


}
