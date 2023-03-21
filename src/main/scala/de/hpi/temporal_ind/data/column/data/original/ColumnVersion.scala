package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.column.data.encoded.ColumnVersionEncoded
import de.hpi.temporal_ind.data.column.io.Dictionary
import de.hpi.temporal_ind.util.Util

import java.io.{File, PrintWriter}
import scala.jdk.CollectionConverters.{SetHasAsJava, SetHasAsScala}

case class ColumnVersion(revisionID: String,
                         revisionDate: String,
                         values: Set[String],
                         header:Option[String],
                         position:Option[Int],
                         columnNotPresent:Boolean) extends AbstractColumnVersion[String]{
  def toKryoSerializableColumnHistory = {
    val valueSet = new java.util.HashSet[String]()
    values.foreach(valueSet.add(_))
    val res = new KryoSerializableColumnVersion()
    res.revisionID=revisionID
    res.revisionDate=revisionDate
    res.values=valueSet
    res.header=header.getOrElse(null)
    res.columnNotPresent=columnNotPresent
    res
  }


  def applyDictionary(dict: Dictionary): ColumnVersionEncoded = {
    ColumnVersionEncoded(revisionID,revisionDate,values.map(v => dict.allValues(v)),columnNotPresent)
  }

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

  def COLUMN_DELETE(revisionID: String, revisionDate: String): ColumnVersion = ColumnVersion(revisionID,revisionDate,Set(),None,None,true)

  def fromKryoSerializableColumnHistory(kryo:KryoSerializableColumnVersion) = {
    ColumnVersion(kryo.revisionID,
      kryo.revisionDate,
      kryo.values.asScala.toSet,
      if(kryo.header==null) None else Some(kryo.header),
      if(kryo.position== -1) None else Some(kryo.position),
      kryo.columnNotPresent
    )
  }
}
