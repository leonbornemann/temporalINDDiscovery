package de.hpi.temporal_ind.discovery

import java.time.Instant

case class QueryStatRow(queryNumber: Int, query: EnrichedColumnHistory, timeInMS: Double, indexType: String, inputSize: Int, outputSize: Int, begin: Option[Instant], end: Option[Instant], indexOrder:Option[Int]) {

  def toCSVLine() = {
    Seq(queryNumber,
      query.och.pageID,
      query.och.tableId,
      query.och.id,
      indexType,
      timeInMS,
      inputSize,
      outputSize,
      begin.getOrElse("None"),
      end.getOrElse("None"),
      indexOrder.getOrElse(-1)
    ).mkString(",")
  }
}
object QueryStatRow {

  def schema = s"queryNumber,pageID,tableID,id,indexType,timeInMS,inputSize,outputSize,begin,end,indexOrder"
}
