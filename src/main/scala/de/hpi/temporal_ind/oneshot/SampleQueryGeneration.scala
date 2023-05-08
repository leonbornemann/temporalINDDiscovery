package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.discovery.input_data.InputDataManager

import java.io.PrintWriter
import scala.util.Random

object SampleQueryGeneration extends App{

  val random = new Random(13)
  val sourceFileBinary = args(0)
  val dataLoader = new InputDataManager(sourceFileBinary, None)
  val data = dataLoader.loadData()
  for(i <- (0 until 10)){
    val target = new PrintWriter(s"sampleQueries_$i.jsonl")
    val shuffled = random.shuffle(data)
    val sample = shuffled.take(10000).map(och => och.columnHistoryID).foreach(s => s.appendToWriter(target))
    target.close()
  }

}
