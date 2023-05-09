package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

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

object WeightedShuffledTimestamps extends JsonReadable[WeightedShuffledTimestamps]
