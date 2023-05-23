package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod

import java.io.File
import java.time.Instant

case class WeightedShuffledTimestamps(shuffledAsString:IndexedSeq[(String,Int)]) extends JsonWritable[WeightedShuffledTimestamps]{

  var shuffledVar:Option[IndexedSeq[(Instant,Int)]] = None

  def shuffled = {
    if(shuffledVar.isEmpty){
      shuffledVar = Some(shuffledAsString.map(t => (Instant.parse(t._1),t._2)))
    }
    shuffledVar.get
  }
}

object WeightedShuffledTimestamps extends JsonReadable[WeightedShuffledTimestamps] {
  def getImportFile(metaDir: File, seed: Long, timeSliceChoiceMethod: TimeSliceChoiceMethod.Value) = {
    if(timeSliceChoiceMethod != TimeSliceChoiceMethod.DYNAMIC_WEIGHTED_RANDOM)
      new File(metaDir.getAbsolutePath + s"/$seed.json")
    else
      new File(metaDir.getAbsolutePath + s"/${seed}_$timeSliceChoiceMethod.json")
  }

}
