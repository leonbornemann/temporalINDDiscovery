package de.hpi.temporal_ind.data.column

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.Util

import java.io.{File, PrintWriter}
import java.time.Instant

case class ColumnVersion(revisionID: String,
                         revisionDate: String,
                         values: Set[String],
                         columnNotPresent:Boolean){

  if(columnNotPresent)
    assert(values.isEmpty)

  def timestamp = Util.instantFromWikipediaDateTimeString(revisionDate)

  def isDelete = columnNotPresent

}

object ColumnVersion {

  def serializeToTable(columnsWithEmpty: IndexedSeq[ColumnVersion],headersWithEmpty:IndexedSeq[String], tableFile: File) = {
    assert(columnsWithEmpty.size == headersWithEmpty.size)
    val columnsWithHeader = columnsWithEmpty.zip(headersWithEmpty)
      .filter(!_._1.isDelete)
    if(!columnsWithHeader.isEmpty) {
      val nrows = columnsWithHeader.map(_._1.values.size).max
      val pr = new PrintWriter(tableFile)
      pr.println(columnsWithHeader.map(_._2).mkString(","))
      val iterators = columnsWithHeader.map(t => t._1.values.iterator)
        .zipWithIndex
      (0 until nrows).foreach(rID => {
        val thisRow = iterators
          .map { case (it, i) => if (it.hasNext) it.next() else columnsWithHeader(i)._1.values.head }
          .map(s => Util.makeStringCSVSafe(s))
          .mkString(",")
        pr.println(thisRow)
      })
      pr.close()
    }
  }

  def INITIALEMPTYID: String = "-1"

  def COLUMN_DELETE(revisionID: String, revisionDate: String): ColumnVersion = ColumnVersion(revisionID,revisionDate,Set(),true)
}
