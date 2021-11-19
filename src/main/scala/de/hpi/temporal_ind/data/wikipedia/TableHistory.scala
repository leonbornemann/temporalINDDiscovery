package de.hpi.temporal_ind.data.wikipedia
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.data.column.{ColumnHistory, ColumnVersion}

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
        .map(values => ColumnVersion(tv.revisionID, tv.revisionDate, values.toSet))
      assert(columnVersions.size == tv.artificialColumnHeaders.size)
      tv.artificialColumnHeaders
        .zip(columnVersions)
        .foreach { case (id, version) =>
          val curHistory = colIDToColHistory.getOrElseUpdate(id, ArrayBuffer[ColumnVersion]())
          if (curHistory.isEmpty || curHistory.last.values != version.values)
            curHistory.append(version)
        }
      //process non-present columns
      val curColumnIds = tv.artificialColumnHeaders.toSet
      colIDToColHistory.keySet.diff(curColumnIds).foreach(id => {
        val history = colIDToColHistory(id)
        if (!history.last.isEmpty) {
          history.append(ColumnVersion.EMPTY(tv.revisionID, tv.revisionDate))
        }
      })
    })
    colIDToColHistory
      .map { case (id, history) => ColumnHistory(id, tableID, pageID, pageTitle, history)}
      .toIndexedSeq
  }
}
object TableHistory extends JsonReadable[TableHistory]
