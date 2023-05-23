package de.hpi.temporal_ind.discovery.indexing

object TimeSliceChoiceMethod extends Enumeration {

  type TimeSliceChoiceMethod = Value

  val RANDOM,BESTX,WEIGHTED_RANDOM,DYNAMIC_WEIGHTED_RANDOM = Value
}
