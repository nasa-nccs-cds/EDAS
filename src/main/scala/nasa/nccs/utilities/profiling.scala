package nasa.nccs.utilities
import java.util.{ArrayList, Collections}

import nasa.nccs.edas.engine.spark.CDSparkContext
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}
import scala.collection.concurrent.TrieMap
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


case class EventRecord( eventId: String, timestamp: Long, duration: Long, clocktime: Long )  extends Serializable  {}

case class StartEvent( eventId: String )  extends Serializable {
  private var _timestamp = System.nanoTime()
  private val _clocktime = System.currentTimeMillis()
  def update(): StartEvent = { _timestamp = System.nanoTime(); this }
  def timestamp: Long = _timestamp
  def clocktime = _clocktime
}

class EventMetrics( val eventId: String ) extends Serializable {
  private var _sumDuration: Float=0.0f
  private var _nEvents: Int=0
  private var _maxDuration: Float=0f
  private var _minDuration: Float=Float.MaxValue
  private var _start: Long=0
  private var _clock: Long=0
  private var _end: Long=0

  def +=( rec: EventRecord ): Unit = {
    val tsec = rec.duration / 1.0e9f
    _sumDuration += tsec
    _nEvents += 1
    if( tsec > _maxDuration ) { _maxDuration = tsec }
    if( tsec < _minDuration ) { _minDuration = tsec }
    if( _start == 0 ) { _start = rec.timestamp }
    if( _clock == 0 ) { _clock = rec.clocktime }
    _end = rec.timestamp + rec.duration
  }
  def clock = _clock
  def toString( baseClockTime: Long ): String = s"[ EM(${eventId}): SumDuration=${_sumDuration}, AveDuration=${_sumDuration/_nEvents}, NEvents=${_nEvents}, MaxDuration=${_maxDuration}, MinDuration=${_minDuration}, Extent=${(_end-_start)/1.0e9} Clock=${(_clock-baseClockTime)/1000.0}]"
}

class EventAccumulator( initActivationStatus: String = "active" ) extends AccumulatorV2[EventRecord, java.util.List[EventMetrics]] with Loggable {
  private var _activationStatus: String = initActivationStatus
  private val _metricsList = new TrieMap[String,EventMetrics]
  private val _startEventList = new TrieMap[String,StartEvent]
  override def isZero: Boolean = _metricsList.isEmpty
  override def reset(): Unit = _metricsList.clear()
  private def newEvent( eventId: String ): EventMetrics = { val newMetrics =  new EventMetrics( eventId ); _metricsList += ( eventId -> newMetrics); newMetrics }
  private def newStartEvent( eventId: String ): StartEvent = { val newStartEvent =  new StartEvent( eventId ); _startEventList += ( eventId -> newStartEvent); newStartEvent }
  private def getMetrics( eventId: String ): EventMetrics = _metricsList.getOrElse( eventId, newEvent(eventId) )
  private def getStartEvent( eventId: String ): Option[StartEvent] = _startEventList.get( eventId )
  private def updateStartEvent( eventId: String ): StartEvent = getStartEvent(eventId).fold( newStartEvent(eventId) )( _.update() )
  override def add(v: EventRecord): Unit = getMetrics( v.eventId ) += v
  override def copyAndReset(): EventAccumulator = new EventAccumulator(_activationStatus)
  override def value: java.util.List[EventMetrics] = java.util.Collections.unmodifiableList( _metricsList.values.toList )
  def mapValue: Map[String,EventMetrics] = _metricsList.toMap
  def setActivationStatus( aStatus: String ) = { _activationStatus = aStatus }

  override def toString(): String = try {
    val events: List[EventMetrics] = value.toList.sortBy( _.clock )
    val baseClockTime = events.head.clock
    "EVENTS:\n ** " + events.map(_.toString(baseClockTime)).mkString( "\n ** ")
  } catch { case err: Throwable => "" }

  def startEvent( eventId: String ): StartEvent = updateStartEvent( eventId )
  def activated: Boolean  = ! _activationStatus.isEmpty

  def endEvent( eventId: String ): Unit = getStartEvent( eventId ) match {
    case Some(startEvent) => add( new EventRecord( eventId, startEvent.timestamp, System.nanoTime()-startEvent.timestamp, startEvent.clocktime ) )
    case None => logger.error(s"End event '${eventId}' without start event in current thread")
  }

  override def copy(): EventAccumulator = {
    val newAcc = new EventAccumulator( _activationStatus )
    newAcc._metricsList.addAll(_metricsList)
    newAcc
  }
  override def merge( other: AccumulatorV2[EventRecord, java.util.List[EventMetrics]] ): Unit = other match {
    case o: EventAccumulator => _metricsList.addAll( o.mapValue )
    case _ => throw new UnsupportedOperationException( s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def profile[T]( eventId: String )( code: () => T ) : T = if( activated ) {
    startEvent(eventId)
    val rv = code()
    endEvent(eventId)
    rv
  } else { code() }
}

// sbt "run-main nasa.nccs.utilities.ClockTest"

object ClockTest1 {
  def main(args : Array[String]) {
    val profiler = new EventAccumulator("active")
    val sc = CDSparkContext()
    sc.sparkContext.register( profiler, "EDAS_EventAccumulator" )
    val indices: RDD[Int] = sc.sparkContext.parallelize( Array.range(0,19), 20 )
    profiler.profile("master") ( ( ) => {
      indices.map(index => {
        profiler.profile(index.toString)(() => {
          Thread.sleep(1000)
        })
      })
    })
    print( profiler.toString() )
  }
}

object ClockTest {
  def main(args : Array[String]) {
    val sc = CDSparkContext()
    val indices: RDD[Int] = sc.sparkContext.parallelize( Array.range(0,19) )
    val times: RDD[String] = indices.map(index => { System.currentTimeMillis().toString } )
    val clock_times: Array[String] = times.collect()
    print( "\n" + clock_times.mkString("\n") )
  }
}





