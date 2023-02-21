package de.hpi.temporal_ind.discovery

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver
import de.metanome.algorithm_integration.results.InclusionDependency

import java.lang

class INDResultCounter() extends InclusionDependencyResultReceiver{

  var counter = 0
  override def receiveResult(inclusionDependency: InclusionDependency): Unit = counter+=1

  override def acceptedResult(inclusionDependency: InclusionDependency): lang.Boolean = true
}
