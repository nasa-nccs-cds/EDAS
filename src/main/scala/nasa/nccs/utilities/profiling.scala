package nasa.nccs.utilities
import java.util.{ArrayList, Collections}
import org.apache.spark._
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


case class EventRecord( eventId: String, timestamp: Long, duration: Long )  extends Serializable  {}

case class StartEvent( eventId: String )  extends Serializable {
  private var _timestamp = System.nanoTime()
  def update(): StartEvent = { _timestamp = System.nanoTime(); this }
  def timestamp: Long = _timestamp
}

class EventMetrics( val eventId: String ) extends Serializable {
  private var _sumDuration: Float=0.0f
  private var _nEvents: Int=0
  private var _maxDuration: Float=0f
  private var _minDuration: Float=Float.MaxValue
  private var _start: Long=0
  private var _end: Long=0

  def +=( rec: EventRecord ): Unit = {
    val tsec = rec.duration / 1.0e9f
    _sumDuration += tsec
    _nEvents += 1
    if( tsec > _maxDuration ) { _maxDuration = tsec }
    if( tsec < _minDuration ) { _minDuration = tsec }
    if( _start == 0 ) { _start = rec.timestamp }
    _end = rec.timestamp + rec.duration
  }
  override def toString: String = s"[ EM(${eventId}): SumDuration=${_sumDuration}, AveDuration=${_sumDuration/_nEvents}, NEvents=${_nEvents}, MaxDuration=${_maxDuration}, MinDuration=${_minDuration}, Extent=${(_end-_start)/1.0e9} ]"
}

class EventAccumulator extends AccumulatorV2[EventRecord, java.util.List[EventMetrics]] with Loggable {
  private val _metricsList: java.util.List[EventMetrics] = Collections.synchronizedList(new ArrayList[EventMetrics]())
  private val _startEventList: java.util.List[StartEvent] = Collections.synchronizedList(new ArrayList[StartEvent]())
  override def isZero: Boolean = _metricsList.isEmpty
  override def reset(): Unit = _metricsList.clear()
  private def newEvent( eventId: String ): EventMetrics = { val newMetrics =  new EventMetrics( eventId ); _metricsList += newMetrics; newMetrics }
  private def newStartEvent( eventId: String ): StartEvent = { val newStartEvent =  new StartEvent( eventId ); _startEventList += newStartEvent; newStartEvent }
  private def getMetrics( eventId: String ): EventMetrics = _metricsList.find( _.eventId.equals(eventId) ).getOrElse( newEvent(eventId) )
  private def getStartEvent( eventId: String ): Option[StartEvent] = _startEventList.find( _.eventId.equals(eventId) )
  private def updateStartEvent( eventId: String ): StartEvent = getStartEvent(eventId).fold( newStartEvent(eventId) )( _.update() )
  override def add(v: EventRecord): Unit = getMetrics( v.eventId ) += v
  override def copyAndReset(): EventAccumulator = new EventAccumulator
  override def value: java.util.List[EventMetrics] = _metricsList.synchronized { java.util.Collections.unmodifiableList(new ArrayList[EventMetrics](_metricsList)) }

  def startEvent( eventId: String ): StartEvent = updateStartEvent( eventId )

  def endEvent( eventId: String ): Unit = getStartEvent( eventId ) match {
    case Some(startEvent) => add( new EventRecord( eventId, startEvent.timestamp, System.nanoTime()-startEvent.timestamp ) )
    case None => logger.error(s"End event '${eventId}' without start event in current thread")
  }

  override def copy(): EventAccumulator = {
    val newAcc = new EventAccumulator
    _metricsList.synchronized { newAcc._metricsList.addAll(_metricsList) }
    newAcc
  }
  override def merge( other: AccumulatorV2[EventRecord, java.util.List[EventMetrics]] ): Unit = other match {
    case o: EventAccumulator => _metricsList.addAll(o.value)
    case _ => throw new UnsupportedOperationException( s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def profile[T]( eventId: String )( code: () => T ) : T = {
    startEvent(eventId)
    val rv = code()
    endEvent(eventId)
    rv
  }
}


