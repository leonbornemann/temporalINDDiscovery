package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory

import java.io.File

object FileFinder extends App {
  val dir = args(0)
  val pageIDs = args(1).split(";")
  val index = ColumnHistory.getIndexForFilesInDir(new File(dir))
  pageIDs.foreach(s => {
    println(s)
    println(index.getFileForPage(BigInt(s)).getAbsolutePath)
  })

}
