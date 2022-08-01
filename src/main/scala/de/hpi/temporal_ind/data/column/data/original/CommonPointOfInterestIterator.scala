package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.temporal.ChronoUnit

class CommonPointOfInterestIterator[T](fkCandidate: AbstractOrderedColumnHistory[T], pkCandidate: AbstractOrderedColumnHistory[T]) extends Iterator[ChangePoint[T]] {

  val allInstants = (fkCandidate.history.versions.keySet ++ pkCandidate.history.versions.keySet)
    .toIndexedSeq
  var curI = 0

  override def hasNext: Boolean = curI!=allInstants.size

  override def next(): ChangePoint[T] = {
    val curInstant = allInstants(curI)
    val end = if(curI+1==allInstants.size) GLOBAL_CONFIG.lastInstant else allInstants(curI+1)
    val timeToNExt = ChronoUnit.MILLIS.between(curInstant,end)
    ChangePoint(curInstant,fkCandidate.versionAt(curInstant),pkCandidate.versionAt(curInstant),timeToNExt)
  }
}
