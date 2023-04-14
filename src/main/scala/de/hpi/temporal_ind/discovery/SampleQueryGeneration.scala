package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.discovery.input_data.InputDataManager

import java.io.PrintWriter
import scala.util.Random

object SampleQueryGeneration extends App{

  val random = new Random(13)
  val target = new PrintWriter("sampleQueries.jsonl")
  val sourceFileBinary = args(0)
  val dataLoader = new InputDataManager(sourceFileBinary, None)
  val data = dataLoader.loadData()
  val sample = random.shuffle(data).take(10000).map(och => och.columnHistoryID).foreach(s => s.appendToWriter(target))
  target.close()

}
