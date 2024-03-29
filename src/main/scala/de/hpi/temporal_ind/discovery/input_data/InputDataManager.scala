package de.hpi.temporal_ind.discovery.input_data

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoException}
import de.hpi.temporal_ind.data.attribute_history.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.attribute_history.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.KryoSerializableColumnHistory
import de.hpi.temporal_ind.util.TimeUtil

import java.io.{File, FileInputStream, FileOutputStream}
import java.time.Instant

class InputDataManager(binaryFile: String,jsonSourceDirs:Option[IndexedSeq[File]]=None) {
  def setFilter(function: OrderedColumnHistory => Boolean) = {
    filterFunction = Some(function)
  }

  var filterFunction:Option[OrderedColumnHistory => Boolean] = None

  def loadData() = {
    if (jsonSourceDirs.isDefined) {
      val (histories, timeLoadingJson) = TimeUtil.executionTimeInMS(loadDataFromJson())
      println(s"Data Loading Json,$timeLoadingJson")
      if(filterFunction.isEmpty)
        histories
      else
        histories.filter(filterFunction.get)
    } else {
      val (histories, timeLoadingBinary) = TimeUtil.executionTimeInMS(loadAsBinary(binaryFile))
      val res = histories.toIndexedSeq
      println(s"Data Loading Binary,$timeLoadingBinary")
      if (filterFunction.isEmpty)
        res
      else
        res.filter(filterFunction.get)
    }
  }

  val kryo = new Kryo();
  kryo.setRegistrationRequired(false)
  kryo.setReferences(true)

  def loadAsBinary(path: String):Iterator[OrderedColumnHistory] = {
    val is = new Input(new FileInputStream(path))
    val it = new Iterator[OrderedColumnHistory]() {


      var curElem = kryo.readClassAndObject(is)

      override def hasNext: Boolean = curElem!=null

      override def next(): OrderedColumnHistory = {
        val cur = curElem
        try{
          curElem=kryo.readClassAndObject(is)
        } catch {
          case e:KryoException => curElem=null
          case e:Throwable => throw(e)
        }
        if(!hasNext){
          is.close()
        }
        OrderedColumnHistory.fromKryoSerializableColumnHistory(cur.asInstanceOf[KryoSerializableColumnHistory])
      }
    }
    it
  }

  def serializeAsBinary(histories: IndexedSeq[OrderedColumnHistory], path: String) = {
    val os = new Output(new FileOutputStream(new File(path)))
    histories
      .map(och => och.toKryoSerializableColumnHistory)
      .foreach(k => kryo.writeClassAndObject(os,k))
//    val list = new java.util.ArrayList[KryoSerializableColumnHistory]()
//    historySerializable.foreach(list.add(_))
//    kryo.writeClassAndObject(os, list)
    //os.write(kryo.toBytesWithClass(histories.toBuffer))//.writeClassAndObject(os,histories.toBuffer)
    kryo.writeClassAndObject(os,histories.last)
    os.close()
  }

  private def loadDataFromJson() = {
    val (histories, timeDataLoading) = TimeUtil.executionTimeInMS(loadJsonHistories())
    histories
  }

  def loadJsonHistories() =
    jsonSourceDirs.get
      .flatMap(sourceDir => OrderedColumnHistory
        .readFromFiles(sourceDir)
        .toIndexedSeq)

}
