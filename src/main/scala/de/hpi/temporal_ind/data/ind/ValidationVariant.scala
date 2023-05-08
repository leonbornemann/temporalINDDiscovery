package de.hpi.temporal_ind.data.ind

object ValidationVariant extends Enumeration {

  // Define a new enumeration with a type alias and work with the full set of enumerated values
  type NormalizationVariant = Value
  val FULL_TIME_PERIOD, LHS_ONLY, RHS_ONLY, LHS_INTERSECT_RHS, LHS_UNION_RHS = Value
}
