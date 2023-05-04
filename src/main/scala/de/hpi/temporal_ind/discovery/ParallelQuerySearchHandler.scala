package de.hpi.temporal_ind.discovery

import java.util.concurrent.{Executors, Semaphore}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}

class ParallelQuerySearchHandler(nthreads:Int,totalNFutures:Int) {

  private val service = Executors.newFixedThreadPool(nthreads)
  val context = ExecutionContext.fromExecutor(service)
  //val futures = new java.util.concurrent.ConcurrentHashMap[String, Future[Any]]()
  var remainingFutureTerminations = new AtomicInteger(totalNFutures)
  var currentlyCreatedFutures = 0
  val allTerminated = new Semaphore(0)

  def addAsFuture[R](block: => R){
    if(currentlyCreatedFutures==totalNFutures){
      throw new AssertionError(s"Cannot create new future, total number of futures $totalNFutures already reached")
    }
    val f = Future(block)(context)
    f.onComplete(_ => {
      val res = remainingFutureTerminations.decrementAndGet()
      //println("State of res",res)
      if(res==0){
        println("Releasing")
        allTerminated.release()
      }
    })(context)
    currentlyCreatedFutures += 1
  }

  def awaitTermination = {
    allTerminated.acquire(1)
    service.shutdown()
  }
}
