package de.hpi.temporal_ind.discovery

import java.util.concurrent.{ExecutorService, Executors, Semaphore}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class ParallelExecutionHandler(totalNFutures:Int) {

  //val futures = new java.util.concurrent.ConcurrentHashMap[String, Future[Any]]()
  var remainingFutureTerminations = new AtomicInteger(totalNFutures)
  var currentlyCreatedFutures = 0
  val allTerminated = new Semaphore(0)

  def addAsFuture[R](block: => R):Future[R] = {
    if(currentlyCreatedFutures==totalNFutures){
      throw new AssertionError(s"Cannot create new future, total number of futures $totalNFutures already reached")
    }
    val f = Future(block)(ParallelExecutionHandler.context)
    f.onComplete(_ => {
      val res = remainingFutureTerminations.decrementAndGet()
      //println("State of res",res)
      if(res==0){
        allTerminated.release()
      }
    })(ParallelExecutionHandler.context)
    currentlyCreatedFutures += 1
    f
  }

  def awaitTermination() = {
    allTerminated.acquire(1)
  }
}

object ParallelExecutionHandler{

  var service: ExecutorService = null
  var context: ExecutionContextExecutor = null
  def initContext(nThreads:Int): Unit = {
    if(service != null){
      service.shutdown()
    }
    service = Executors.newFixedThreadPool(nThreads)
    context = ExecutionContext.fromExecutor(service)
  }
}
