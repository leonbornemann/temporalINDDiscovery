package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory

import java.io.File

class IndexedColumnHistories(histories:IndexedSeq[ColumnHistory]) {

  //index from pageID to columnID
  val multiLevelIndex = histories
    .groupBy(ch => ch.pageID)
    .map{case (pID,histories) =>
      if(pID=="49621038")
        println()
      (pID,histories.map(ch => (ch.id,ch)).toMap)}
}
object IndexedColumnHistories {

  def lowerIDBound(f: File) = f.getName.split("xml-p")(1).split("p")(0).toLong

  def upperIDBound(f: File) = f.getName.split("_")(0).split("p").last.toLong

  def getFileForID(dir: File, id: Long):File = {
    //enwiki-20171103-pages-meta-history1xml-p7841p9534_wikitableHistories.json
    val file = dir
      .listFiles()
      .find(f => id >= lowerIDBound(f) && id <= upperIDBound(f))
      .get
    println(s"Loading file $file")
    file
  }

  def loadForPageIDS(dir:File, pageIDs:IndexedSeq[Long]) = {
    val histories = pageIDs
      .flatMap(id => ColumnHistory.fromJsonObjectPerLineFile(getFileForID(dir,id).getAbsolutePath).toIndexedSeq)
    new IndexedColumnHistories(histories)
  }

  def fromColumnHistoryJsonPerLineDir(dir:String) = {
    new IndexedColumnHistories(ColumnHistory.iterableFromJsonObjectPerLineDir(new File(dir),true).toIndexedSeq)
  }

}
