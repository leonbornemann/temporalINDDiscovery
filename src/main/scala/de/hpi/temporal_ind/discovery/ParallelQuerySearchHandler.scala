package de.hpi.temporal_ind.discovery

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

class ParallelQuerySearchHandler(nthreads:Int) {

  private val service = Executors.newFixedThreadPool(nthreads)
  val context = ExecutionContext.fromExecutor(service)
  val futures = new java.util.concurrent.ConcurrentHashMap[String, Future[Any]]()

}
