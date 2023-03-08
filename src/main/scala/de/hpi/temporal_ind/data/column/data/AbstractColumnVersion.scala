package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.util.Util

abstract class AbstractColumnVersion[T] {

  if(columnNotPresent)
    assert(values.isEmpty)

  def timestamp = Util.instantFromWikipediaDateTimeString(revisionDate)

  def isDelete = columnNotPresent

  def revisionID: String
  def revisionDate: String
  def values: Set[T]
  def columnNotPresent:Boolean
  def header:Option[String]
  def position:Option[Int]
  //header:Option[String],
  //                         position:Option[Int],
}

object AbstractColumnVersion {
  def getEmpty[T](): AbstractColumnVersion[T] = new AbstractColumnVersion[T] {
    override def revisionID: String = ""

    override def revisionDate: String = ""

    override def values: Set[T] = Set()

    override def columnNotPresent: Boolean = true

    override def header: Option[String] = None

    override def position: Option[Int] = None
  }

  def INITIALEMPTYID: String = "-1"
}
