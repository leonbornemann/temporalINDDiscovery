package de.hpi.temporal_ind.discovery.indexing

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver
import de.metanome.algorithm_integration.results.InclusionDependency

import java.lang

/***
 * Dummy class that we pass to MANY, because the parameters require a result receiver
 */
class INDResultCounter() extends InclusionDependencyResultReceiver{

  var counter = 0
  override def receiveResult(inclusionDependency: InclusionDependency): Unit = counter+=1

  override def acceptedResult(inclusionDependency: InclusionDependency): lang.Boolean = true
}
