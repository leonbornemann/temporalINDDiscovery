package de.hpi.temporal_ind.data.wikipedia
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.data.column.data.original.{ColumnHistory, ColumnVersion}

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class TableHistory(pageID:String,
                        pageTitle:String,
                        tableID:String,
                        tables:IndexedSeq[TableVersion]) extends JsonWritable[TableHistory]{

  def withoutHeader: TableHistory = {
    val tablesNew = tables.map(tv => tv.withOutHeader)
    TableHistory(pageID,pageTitle, tableID, tablesNew)
  }

  def extractColumnHistories = {
    val colIDToColHistory = mutable.HashMap[String, ArrayBuffer[ColumnVersion]]()
    tables.foreach(tv => {
      val tableState = if(tv.cells.size>0){
        val expandedRatioHeader = tv.cells(0).filter(_.wasExpanded).size / tv.cells(0).size.toDouble
        val expandedRatioMiddle = tv.cells(tv.cells.size / 2).filter(_.wasExpanded).size / tv.cells(0).size.toDouble
        val expandedRatioEnd = tv.cells(tv.cells.size / 2).filter(_.wasExpanded).size / tv.cells(0).size.toDouble
        val filterExpanded = expandedRatioHeader>0.5 && expandedRatioMiddle>0.5 && expandedRatioEnd > 0.5
        val tableState = tv.getTableState(filterExpanded)
        tableState.artificialHeaders
          .zip(tableState.columns)
          .foreach { case (id, version) =>
            val curHistory = colIDToColHistory.getOrElseUpdate(id, ArrayBuffer[ColumnVersion]())
            if (curHistory.isEmpty || curHistory.last.values != version.values)
              curHistory.append(version)
          }
        tableState
      } else {
        TableState(IndexedSeq(),IndexedSeq())
      }
      //process non-present columns in this version
      val curColumnIds = tableState.artificialHeaders.toSet
      colIDToColHistory.keySet.diff(curColumnIds).foreach(id => {
        val history = colIDToColHistory(id)
        if (!history.last.isDelete) {
          history.append(ColumnVersion.COLUMN_DELETE(tv.revisionID, tv.revisionDate))
        }
      })
    })
    val columnHistories = colIDToColHistory
      .map { case (id, history) => ColumnHistory(id, tableID, pageID, pageTitle, history)}
      .toIndexedSeq
    if(colIDToColHistory.contains("e87339ca-aded-4806-8f62-40a6a14a73b6")){
      println()
    }
    columnHistories
  }
}
object TableHistory extends JsonReadable[TableHistory]
