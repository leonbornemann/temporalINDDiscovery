package de.hpi.temporal_ind.data.column.data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories.{lowerIDBound, upperIDBound}
import de.hpi.temporal_ind.data.column.data.original.ColumnHistory

import java.io.File
import scala.util.Random

class IncrementalIndexedColumnHistories(dir:File) extends StrictLogging{
  def getRandomColumnHistory() = {
    val chosenFile = dir.listFiles().toIndexedSeq(random.nextInt(dir.listFiles().size))
    val map = load(chosenFile)
    val a = map.keySet.toIndexedSeq(random.nextInt(map.keySet.size))
    map(a)
  }

  val index = collection.mutable.HashMap[File, Map[String, ColumnHistory]]()
  var maxMem = 100
  val random = new Random(13)

  def getOrLoad(pageID:Long,columnID:String) = {
    val file = getFileForPageID(pageID)
    val mapInFile = load(file)
    val result = mapInFile(columnID)
    if(index.size>maxMem) {
      val toRemove = index.keySet.toIndexedSeq(random.nextInt(index.keySet.size))
      logger.debug(s"Loaded $file, dropped $toRemove")
      index.remove(toRemove)
    }
    result
  }

  private def load(file: File) = {
    index.getOrElseUpdate(file, ColumnHistory.fromJsonObjectPerLineFile(file.getAbsolutePath).map(ch => (ch.id, ch)).toMap)
  }

  def getFileForPageID(id: Long): File = {
    //enwiki-20171103-pages-meta-history1xml-p7841p9534_wikitableHistories.json
    val file = dir
      .listFiles()
      .find(f => id >= lowerIDBound(f) && id <= upperIDBound(f))
      .get
    file
  }


}
