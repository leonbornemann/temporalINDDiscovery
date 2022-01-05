package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.column.data.original.{ColumnHistory, OrderedColumnHistory}

import java.io.File

class ColumnHistoryBuffer {

  val loadedFiles = collection.mutable.HashMap[File,Map[String,OrderedColumnHistory]]()

  def readColumnHistoryMap(file: File): Map[String, OrderedColumnHistory] = {
    ColumnHistory.fromJsonObjectPerLineFile(file.getAbsolutePath)
      .map(ch => (ch.id,ch.asOrderedVersionMap))
      .toMap
  }

  def getOrLoadHistory(file:File) = loadedFiles.getOrElseUpdate(file,readColumnHistoryMap(file))

}
