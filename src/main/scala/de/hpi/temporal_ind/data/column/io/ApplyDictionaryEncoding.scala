package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory

import java.io.{File, PrintWriter}

object ApplyDictionaryEncoding extends App {
  val inputPath = args(0)
  val dictPath = args(1)
  val targetPath = args(2)
  val dictionary = Dictionary.fromJsonFile(dictPath)
  new File(inputPath).listFiles().sortBy(_.getName).foreach { f =>
    println(s"Reading ${f.getName}")
    val histories = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val historiesEncoded = histories.map(ch => ch.applyDictionary(dictionary))
    val targetFile = targetPath + "/" + f.getName
    val pr = new PrintWriter(targetFile)
    historiesEncoded.foreach(ch => ch.appendToWriter(pr))
    pr.close()
  }
}
