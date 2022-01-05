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
      val columnVersions = tv.getColumns()
        .map(values => ColumnVersion(tv.revisionID, tv.revisionDate, values.toSet,false))
      assert(columnVersions.size == tv.artificialColumnHeaders.size)
      tv.artificialColumnHeaders
        .zip(columnVersions)
        .foreach { case (id, version) =>
          val curHistory = colIDToColHistory.getOrElseUpdate(id, ArrayBuffer[ColumnVersion]())
          if (curHistory.isEmpty || curHistory.last.values != version.values)
            curHistory.append(version)
        }
      //process non-present columns in this version
      val curColumnIds = tv.artificialColumnHeaders.toSet
      colIDToColHistory.keySet.diff(curColumnIds).foreach(id => {
        if(id=="cc3b43ad-0a4f-42ef-92b5-4e902a0672d3"){
          println()
        }
        val history = colIDToColHistory(id)
        if (!history.last.isDelete) {
          history.append(ColumnVersion.COLUMN_DELETE(tv.revisionID, tv.revisionDate))
        }
      })
    })
    colIDToColHistory
      .map { case (id, history) => ColumnHistory(id, tableID, pageID, pageTitle, history)}
      .toIndexedSeq
  }
}
object TableHistory extends JsonReadable[TableHistory]
