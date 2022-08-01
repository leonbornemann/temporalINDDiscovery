package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory

import java.io.File

class IndexedColumnHistories(histories:IndexedSeq[ColumnHistory]) {

  //index from pageID to columnID
  val multiLevelIndex = histories
    .groupBy(ch => ch.pageID)
    .map{case (pID,histories) => (pID,histories.map(ch => (ch.id,ch)).toMap)}
}
object IndexedColumnHistories {

  def fromColumnHistoryJsonPerLineDir(dir:String) = {
    new IndexedColumnHistories(ColumnHistory.iterableFromJsonObjectPerLineDir(new File(dir),true).toIndexedSeq)
  }

}
